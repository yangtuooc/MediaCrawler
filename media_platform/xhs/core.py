import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright, Browser)
from tenacity import RetryError

from base.base_crawler import AbstractCrawler
from config.context import CrawlerContext
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import xhs as xhs_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import XiaoHongShuClient
from .exception import DataFetchError
from .field import SearchSortType
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    context_page: Page
    xhs_client: XiaoHongShuClient
    browser_context: BrowserContext
    login_obj: Optional[XiaoHongShuLogin] = None
    browser: Optional[Browser] = None

    def __init__(self) -> None:
        self.index_url = "https://www.xiaohongshu.com"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    async def start(self, context: CrawlerContext) -> None:
        utils.logger.info(f"[XiaoHongShuCrawler.start] Starting with context: {context}")
        playwright_proxy_format, httpx_proxy_format = None, None
        if context.enable_ip_proxy:
            ip_proxy_pool = await create_ip_pool(context.ip_proxy_pool_count, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            chromium = playwright.chromium
            self.browser = await chromium.launch(headless=False)
            self.browser_context = await self.launch_browser(
                context,
                self.browser,
                playwright_proxy_format,
                self.user_agent
            )
            
            # 添加浏览器关闭事件监听
            self.browser.on("disconnected", lambda: asyncio.create_task(self.on_browser_close()))

            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            await self.browser_context.add_cookies([{
                'name': "webId",
                'value': "xxx123",  # any value
                'domain': ".xiaohongshu.com",
                'path': "/"
            }])
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            self.xhs_client = await self.create_xhs_client(context, httpx_proxy_format)
            if not await self.xhs_client.pong():
                utils.logger.warning("[XiaoHongShuCrawler.start] Pong failed, attempting login")
                self.login_obj = XiaoHongShuLogin(
                    login_type=context.login_type,
                    login_phone="",  # input your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=context.cookies
                )
                await self.login_obj.begin()
                
                # 等待用户手动验证
                await self.wait_for_manual_verification()
                
                await self.xhs_client.update_cookies(browser_context=self.browser_context)
            else:
                utils.logger.info("[XiaoHongShuCrawler.start] Pong successful, user is logged in")

            crawler_type_var.set(context.crawler_type)
            if context.crawler_type == "search":
                await self.search(context)
            elif context.crawler_type == "detail":
                await self.get_specified_notes(context)
            elif context.crawler_type == "creator":
                await self.get_creators_and_notes(context)

        utils.logger.info("[XiaoHongShuCrawler.start] Xhs Crawler finished ...")

    async def on_browser_close(self):
        utils.logger.info("[XiaoHongShuCrawler.on_browser_close] Browser closed, stopping crawler...")
        await self.stop()

    async def search(self, context: CrawlerContext) -> None:
        utils.logger.info("[XiaoHongShuCrawler.search] Begin search xiaohongshu keywords")
        xhs_limit_count = 20  # xhs limit page fixed value
        if context.crawler_max_notes_count < xhs_limit_count:
            context.crawler_max_notes_count = xhs_limit_count
        start_page = context.start_page
        for keyword in context.keywords.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * xhs_limit_count <= context.crawler_max_notes_count:
                if page < start_page:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] search xhs keyword: {keyword}, page: {page}")
                    note_id_list: List[str] = []
                    notes_res = await self.xhs_client.get_note_by_keyword(
                        keyword=keyword,
                        page=page,
                        sort=SearchSortType(context.sort_type) if context.sort_type != '' else SearchSortType.GENERAL,
                    )
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Search notes res:{notes_res}")
                    if not notes_res or not notes_res.get('has_more', False):
                        utils.logger.info("No more content!")
                        break
                    semaphore = asyncio.Semaphore(context.max_concurrency_num)
                    task_list = [
                        self.get_note_detail_async_task(
                            note_id=post_item.get("id"),
                            xsec_source=post_item.get("xsec_source"),
                            xsec_token=post_item.get("xsec_token"),
                            semaphore=semaphore
                        )
                        for post_item in notes_res.get("items", {})
                        if post_item.get('model_type') not in ('rec_query', 'hot_query')
                    ]
                    note_details = await asyncio.gather(*task_list)
                    for note_detail in note_details:
                        if note_detail:
                            await xhs_store.update_xhs_note(note_detail)
                            await self.get_notice_media(context, note_detail)
                            note_id_list.append(note_detail.get("note_id"))
                    page += 1
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Note details: {note_details}")
                    await self.batch_get_note_comments(context, note_id_list)
                    await asyncio.sleep(random.uniform(1, 3))  # 添加随机延迟
                except DataFetchError:
                    utils.logger.error("[XiaoHongShuCrawler.search] Get note detail error")
                    break

    async def get_creators_and_notes(self, context: CrawlerContext) -> None:
        utils.logger.info("[XiaoHongShuCrawler.get_creators_and_notes] Begin get xiaohongshu creators")
        for user_id in context.xhs_creator_id_list:
            creator_info: Dict = await self.xhs_client.get_creator_info(user_id=user_id)
            if creator_info:
                await xhs_store.save_creator(user_id, creator=creator_info)

            all_notes_list = await self.xhs_client.get_all_notes_by_creator(
                user_id=user_id,
                crawl_interval=random.random(),
                callback=self.fetch_creator_notes_detail
            )

            note_ids = [note_item.get("note_id") for note_item in all_notes_list]
            await self.batch_get_note_comments(context, note_ids)

    async def fetch_creator_notes_detail(self, note_list: List[Dict]):
        semaphore = asyncio.Semaphore(self.context.max_concurrency_num)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source=post_item.get("xsec_source"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore
            )
            for post_item in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail:
                await xhs_store.update_xhs_note(note_detail)

    @staticmethod
    def format_proxy_info(ip_proxy_info: IpInfoModel) -> Tuple[Optional[Dict], Optional[Dict]]:
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        return playwright_proxy, httpx_proxy

    async def launch_browser(self, context: CrawlerContext, browser: Browser, playwright_proxy: Optional[Dict], user_agent: Optional[str]) -> BrowserContext:
        utils.logger.info("[XiaoHongShuCrawler.launch_browser] Begin create browser context ...")
        context_options = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": user_agent,
            "proxy": playwright_proxy
        }

        if context.save_login_state:
            user_data_dir = os.path.join(os.getcwd(), "browser_data", context.user_data_dir % context.platform)
            # 使用 browser.new_context() 而不是 browser.launch_persistent_context()
            browser_context = await browser.new_context(**context_options)
            # 如果需要保存登录状态，可以在这里添加额外的逻辑
            # 例如，从文件加载 cookies 或其他状态信息
        else:
            browser_context = await browser.new_context(**context_options)

        await browser_context.add_init_script(path="libs/stealth.min.js")
        return browser_context

    async def create_xhs_client(self, context: CrawlerContext, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        utils.logger.info("[XiaoHongShuCrawler.create_xhs_client] Begin create xiaohongshu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        xhs_client_obj = XiaoHongShuClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Origin": "https://www.xiaohongshu.com",
                "Referer": "https://www.xiaohongshu.com",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return xhs_client_obj

    async def get_notice_media(self, context: CrawlerContext, note_item: Dict):
        if not context.enable_get_images:
            return
        note_id = note_item.get("note_id")
        image_list: List[Dict] = note_item.get("image_list", [])

        for img in image_list:
            if img.get('url_default') != '':
                img.update({'url': img.get('url_default')})

        if not image_list:
            return
        picNum = 0
        for pic in image_list:
            url = pic.get("url")
            if not url:
                continue
            content = await self.xhs_client.get_note_media(url)
            if content is None:
                continue
            extension_file_name = f"{picNum}.jpg"
            picNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

        videos = xhs_store.get_video_url_arr(note_item)

        if not videos:
            return
        videoNum = 0
        for url in videos:
            content = await self.xhs_client.get_note_media(url)
            if content is None:
                continue
            extension_file_name = f"{videoNum}.mp4"
            videoNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

    async def stop(self):
        self.stop_flag = True
        try:
            if hasattr(self, 'xhs_client') and self.xhs_client:
                await self.xhs_client.close()
            if hasattr(self, 'login_obj') and self.login_obj:
                await self.login_obj.stop()
            if hasattr(self, 'browser_context') and self.browser_context:
                await self.browser_context.close()
            if hasattr(self, 'browser') and self.browser:
                await self.browser.close()
        except Exception as e:
            utils.logger.error(f"Error during XiaoHongShuCrawler stop: {str(e)}")
        utils.logger.info("[XiaoHongShuCrawler.stop] Crawler stopped")
