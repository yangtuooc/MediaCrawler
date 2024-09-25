# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/23 15:41
# @Desc    : 微博爬虫主流程代码


import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

from base.base_crawler import AbstractCrawler
from config.context import CrawlerContext
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import weibo as weibo_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import WeiboClient
from .exception import DataFetchError
from .field import SearchType
from .help import filter_search_result_card
from .login import WeiboLogin


class WeiboCrawler(AbstractCrawler):
    context_page: Page
    wb_client: WeiboClient
    browser_context: BrowserContext

    def __init__(self):
        self.index_url = "https://www.weibo.com"
        self.mobile_index_url = "https://m.weibo.cn"
        self.user_agent = utils.get_user_agent()
        self.mobile_user_agent = utils.get_mobile_user_agent()

    async def start(self, context: CrawlerContext):
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
                self.mobile_user_agent
            )
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.mobile_index_url)

            self.wb_client = await self.create_weibo_client(context, httpx_proxy_format)
            if not await self.wb_client.pong():
                login_obj = WeiboLogin(
                    login_type=context.login_type,
                    login_phone="",  # your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=context.cookies
                )
                await login_obj.begin()

                utils.logger.info("[WeiboCrawler.start] redirect weibo mobile homepage and update cookies on mobile platform")
                await self.context_page.goto(self.mobile_index_url)
                await asyncio.sleep(2)
                await self.wb_client.update_cookies(browser_context=self.browser_context)

            crawler_type_var.set(context.crawler_type)
            if context.crawler_type == "search":
                await self.search(context)
            elif context.crawler_type == "detail":
                await self.get_specified_notes(context)
            elif context.crawler_type == "creator":
                await self.get_creators_and_notes(context)

        utils.logger.info("[WeiboCrawler.start] Weibo Crawler finished ...")

    async def search(self, context: CrawlerContext):
        utils.logger.info("[WeiboCrawler.search] Begin search weibo keywords")
        weibo_limit_count = 10  # weibo limit page fixed value
        if context.crawler_max_notes_count < weibo_limit_count:
            context.crawler_max_notes_count = weibo_limit_count
        start_page = context.start_page
        for keyword in context.keywords.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[WeiboCrawler.search] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * weibo_limit_count <= context.crawler_max_notes_count:
                if page < start_page:
                    utils.logger.info(f"[WeiboCrawler.search] Skip page: {page}")
                    page += 1
                    continue
                utils.logger.info(f"[WeiboCrawler.search] search weibo keyword: {keyword}, page: {page}")
                search_res = await self.wb_client.get_note_by_keyword(
                    keyword=keyword,
                    page=page,
                    search_type=SearchType.DEFAULT
                )
                note_id_list: List[str] = []
                note_list = filter_search_result_card(search_res.get("cards"))
                for note_item in note_list:
                    if note_item:
                        mblog: Dict = note_item.get("mblog")
                        if mblog:
                            note_id_list.append(mblog.get("id"))
                            await weibo_store.update_weibo_note(note_item)
                            await self.get_note_images(context, mblog)

                page += 1
                await self.batch_get_notes_comments(context, note_id_list)

    async def get_specified_notes(self, context: CrawlerContext):
        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        task_list = [
            self.get_note_info_task(note_id=note_id, semaphore=semaphore) for note_id in
            context.weibo_specified_id_list
        ]
        video_details = await asyncio.gather(*task_list)
        for note_item in video_details:
            if note_item:
                await weibo_store.update_weibo_note(note_item)
        await self.batch_get_notes_comments(context, context.weibo_specified_id_list)

    async def get_note_info_task(self, note_id: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        async with semaphore:
            try:
                result = await self.wb_client.get_note_info_by_id(note_id)
                return result
            except DataFetchError as ex:
                utils.logger.error(f"[WeiboCrawler.get_note_info_task] Get note detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(f"[WeiboCrawler.get_note_info_task] Get note detail error: {ex}")
                return None

    async def batch_get_notes_comments(self, context: CrawlerContext, note_id_list: List[str]):
        if not context.enable_get_comments:
            utils.logger.info(f"[WeiboCrawler.batch_get_notes_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[WeiboCrawler.batch_get_notes_comments] note ids:{note_id_list}")
        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        task_list: List[Task] = []
        for note_id in note_id_list:
            task = asyncio.create_task(self.get_comments(context, note_id, semaphore), name=note_id)
            task_list.append(task)

        await asyncio.gather(*task_list)

    async def get_comments(self, context: CrawlerContext, note_id: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                utils.logger.info(f"[WeiboCrawler.get_comments] begin get note_id: {note_id} comments ...")
                await self.wb_client.get_note_all_comments(
                    note_id=note_id,
                    callback=weibo_store.batch_update_weibo_note_comments
                )
            except Exception as e:
                utils.logger.error(f"[WeiboCrawler.get_comments] get note_id: {note_id} comments error: {e}")

    async def get_creators_and_notes(self, context: CrawlerContext):
        utils.logger.info("[WeiboCrawler.get_creators_and_notes] Begin get weibo creators")
        for user_id in context.weibo_creator_id_list:
            creator_info: Dict = await self.wb_client.get_user_info(user_id)
            if creator_info:
                await weibo_store.save_creator(user_id, creator=creator_info)

            all_note_list = await self.wb_client.get_all_user_notes(
                user_id=user_id,
                callback=self.fetch_creator_note_detail
            )

            note_ids = [note.get("id") for note in all_note_list]
            await self.batch_get_notes_comments(context, note_ids)

    async def fetch_creator_note_detail(self, note_list: List[Dict]):
        semaphore = asyncio.Semaphore(self.context.max_concurrency_num)
        task_list = [
            self.get_note_info_task(note.get("id"), semaphore) for note in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail is not None:
                await weibo_store.update_weibo_note(note_detail)

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
        utils.logger.info("[WeiboCrawler.launch_browser] Begin create browser context ...")
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

    async def create_weibo_client(self, context: CrawlerContext, httpx_proxy: Optional[str]) -> WeiboClient:
        utils.logger.info("[WeiboCrawler.create_weibo_client] Begin create weibo API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        wb_client_obj = WeiboClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": self.mobile_user_agent,
                "Cookie": cookie_str,
                "Origin": "https://m.weibo.cn",
                "Referer": "https://m.weibo.cn",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return wb_client_obj

    async def get_note_images(self, context: CrawlerContext, note_item: Dict):
        if not context.enable_get_images:
            return
        note_id = note_item.get("id")
        pic_ids = note_item.get("pic_ids", [])
        pic_infos = note_item.get("pic_infos", {})

        if not pic_ids:
            return

        for pic_id in pic_ids:
            pic_info = pic_infos.get(pic_id, {})
            largest_url = pic_info.get("largest", {}).get("url")
            if not largest_url:
                continue
            content = await self.wb_client.get_note_media(largest_url)
            if content is None:
                continue
            extension_file_name = f"{pic_id}.jpg"
            await weibo_store.update_weibo_note_image(note_id, content, extension_file_name)
