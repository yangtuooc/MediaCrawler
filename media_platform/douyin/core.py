import asyncio
import os
import random
from asyncio import Task
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

from base.base_crawler import AbstractCrawler
from config.context import CrawlerContext
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import douyin as douyin_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import DOUYINClient
from .exception import DataFetchError
from .field import PublishTimeType
from .login import DouYinLogin


class DouYinCrawler(AbstractCrawler):
    context_page: Page
    dy_client: DOUYINClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://www.douyin.com"

    async def start(self, context: CrawlerContext) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None
        if context.enable_ip_proxy:
            ip_proxy_pool = await create_ip_pool(context.ip_proxy_pool_count, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(
                context,
                chromium,
                playwright_proxy_format,
                user_agent=None
            )
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            self.dy_client = await self.create_douyin_client(context, httpx_proxy_format)
            if not await self.dy_client.pong(browser_context=self.browser_context):
                login_obj = DouYinLogin(
                    login_type=context.login_type,
                    login_phone="",  # your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=context.cookies
                )
                await login_obj.begin()
                await self.dy_client.update_cookies(browser_context=self.browser_context)
            crawler_type_var.set(context.crawler_type)
            if context.crawler_type == "search":
                await self.search(context)
            elif context.crawler_type == "detail":
                await self.get_specified_awemes(context)
            elif context.crawler_type == "creator":
                await self.get_creators_and_videos(context)

            utils.logger.info("[DouYinCrawler.start] Douyin Crawler finished ...")

    async def search(self, context: CrawlerContext) -> None:
        utils.logger.info("[DouYinCrawler.search] Begin search douyin keywords")
        dy_limit_count = 10  # douyin limit page fixed value
        if context.crawler_max_notes_count < dy_limit_count:
            context.crawler_max_notes_count = dy_limit_count
        start_page = context.start_page  # start page number
        for keyword in context.keywords.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[DouYinCrawler.search] Current keyword: {keyword}")
            aweme_list: List[str] = []
            page = 0
            dy_search_id = ""
            while (page - start_page + 1) * dy_limit_count <= context.crawler_max_notes_count:
                if page < start_page:
                    utils.logger.info(f"[DouYinCrawler.search] Skip {page}")
                    page += 1
                    continue
                try:
                    utils.logger.info(f"[DouYinCrawler.search] search douyin keyword: {keyword}, page: {page}")
                    posts_res = await self.dy_client.search_info_by_keyword(keyword=keyword,
                                                                            offset=page * dy_limit_count - dy_limit_count,
                                                                            publish_time=PublishTimeType(context.publish_time_type),
                                                                            search_id=dy_search_id
                                                                            )
                except DataFetchError:
                    utils.logger.error(f"[DouYinCrawler.search] search douyin keyword: {keyword} failed")
                    break

                page += 1
                if "data" not in posts_res:
                    utils.logger.error(
                        f"[DouYinCrawler.search] search douyin keyword: {keyword} failed，账号也许被风控了。")
                    break
                dy_search_id = posts_res.get("extra", {}).get("logid", "")
                for post_item in posts_res.get("data"):
                    try:
                        aweme_info: Dict = post_item.get("aweme_info") or \
                                           post_item.get("aweme_mix_info", {}).get("mix_items")[0]
                    except TypeError:
                        continue
                    aweme_list.append(aweme_info.get("aweme_id", ""))
                    await douyin_store.update_douyin_aweme(aweme_item=aweme_info)
            utils.logger.info(f"[DouYinCrawler.search] keyword:{keyword}, aweme_list:{aweme_list}")
            await self.batch_get_note_comments(context, aweme_list)

    async def get_specified_awemes(self, context: CrawlerContext):
        """Get the information and comments of the specified post"""
        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        task_list = [
            self.get_aweme_detail(aweme_id=aweme_id, semaphore=semaphore) for aweme_id in context.dy_specified_id_list
        ]
        aweme_details = await asyncio.gather(*task_list)
        for aweme_detail in aweme_details:
            if aweme_detail is not None:
                await douyin_store.update_douyin_aweme(aweme_detail)
        await self.batch_get_note_comments(context, context.dy_specified_id_list)

    async def get_aweme_detail(self, aweme_id: str, semaphore: asyncio.Semaphore) -> Any:
        """Get note detail"""
        async with semaphore:
            try:
                return await self.dy_client.get_video_by_id(aweme_id)
            except DataFetchError as ex:
                utils.logger.error(f"[DouYinCrawler.get_aweme_detail] Get aweme detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(
                    f"[DouYinCrawler.get_aweme_detail] have not fund note detail aweme_id:{aweme_id}, err: {ex}")
                return None

    async def batch_get_note_comments(self, context: CrawlerContext, aweme_list: List[str]) -> None:
        if not context.enable_get_comments:
            utils.logger.info(f"[DouYinCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        task_list: List[Task] = []
        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        for aweme_id in aweme_list:
            task = asyncio.create_task(
                self.get_comments(context, aweme_id, semaphore), name=aweme_id)
            task_list.append(task)
        if len(task_list) > 0:
            await asyncio.wait(task_list)

    async def get_comments(self, context: CrawlerContext, aweme_id: str, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            try:
                await self.dy_client.get_aweme_all_comments(
                    aweme_id=aweme_id,
                    crawl_interval=random.random(),
                    is_fetch_sub_comments=context.enable_get_sub_comments,
                    callback=douyin_store.batch_update_dy_aweme_comments
                )
                utils.logger.info(
                    f"[DouYinCrawler.get_comments] aweme_id: {aweme_id} comments have all been obtained and filtered ...")
            except DataFetchError as e:
                utils.logger.error(f"[DouYinCrawler.get_comments] aweme_id: {aweme_id} get comments failed, error: {e}")

    async def get_creators_and_videos(self, context: CrawlerContext) -> None:
        utils.logger.info("[DouYinCrawler.get_creators_and_videos] Begin get douyin creators")
        for user_id in context.dy_creator_id_list:
            creator_info: Dict = await self.dy_client.get_user_info(user_id)
            if creator_info:
                await douyin_store.save_creator(user_id, creator=creator_info)

            all_video_list = await self.dy_client.get_all_user_aweme_posts(
                sec_user_id=user_id,
                callback=self.fetch_creator_video_detail
            )

            video_ids = [video_item.get("aweme_id") for video_item in all_video_list]
            await self.batch_get_note_comments(context, video_ids)

    async def fetch_creator_video_detail(self, video_list: List[Dict]):
        semaphore = asyncio.Semaphore(self.context.max_concurrency_num)
        task_list = [
            self.get_aweme_detail(post_item.get("aweme_id"), semaphore) for post_item in video_list
        ]

        note_details = await asyncio.gather(*task_list)
        for aweme_item in note_details:
            if aweme_item is not None:
                await douyin_store.update_douyin_aweme(aweme_item)

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

    async def launch_browser(self, context: CrawlerContext, chromium: BrowserType, playwright_proxy: Optional[Dict], user_agent: Optional[str]) -> BrowserContext:
        utils.logger.info("[DouYinCrawler.launch_browser] Begin create browser context ...")
        if context.save_login_state:
            user_data_dir = os.path.join(os.getcwd(), "browser_data", context.user_data_dir % context.platform)
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=context.headless,
                proxy=playwright_proxy,
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context
        else:
            browser = await chromium.launch(headless=context.headless, proxy=playwright_proxy)
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context

    async def create_douyin_client(self, context: CrawlerContext, httpx_proxy: Optional[str]) -> DOUYINClient:
        utils.logger.info("[DouYinCrawler.create_douyin_client] Begin create douyin API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        dy_client_obj = DOUYINClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": utils.get_user_agent(),
                "Cookie": cookie_str,
                "Origin": "https://www.douyin.com",
                "Referer": "https://www.douyin.com",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return dy_client_obj
