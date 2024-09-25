# -*- coding: utf-8 -*-
import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

from base.base_crawler import AbstractCrawler
from config.context import CrawlerContext
from model.m_zhihu import ZhihuContent
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import zhihu as zhihu_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import ZhiHuClient
from .exception import DataFetchError
from .help import ZhiHuJsonExtractor
from .login import ZhiHuLogin


class ZhihuCrawler(AbstractCrawler):
    context_page: Page
    zhihu_client: ZhiHuClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://www.zhihu.com"
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        self._extractor = ZhiHuJsonExtractor()

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
                self.user_agent
            )
            await self.browser_context.add_init_script(path="libs/stealth.min.js")

            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url, wait_until="domcontentloaded")

            self.zhihu_client = await self.create_zhihu_client(context, httpx_proxy_format)
            if not await self.zhihu_client.pong():
                login_obj = ZhiHuLogin(
                    login_type=context.login_type,
                    login_phone="",  # input your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=context.cookies
                )
                await login_obj.begin()
                await self.zhihu_client.update_cookies(browser_context=self.browser_context)

            # 知乎的搜索接口需要打开搜索页面之后cookies才能访问API，单独的首页不行
            utils.logger.info("[ZhihuCrawler.start] Zhihu跳转到搜索页面获取搜索页面的Cookies，改过程需要5秒左右")
            await self.context_page.goto(f"{self.index_url}/search?q=python&search_source=Guess&utm_content=search_hot&type=content")
            await asyncio.sleep(5)
            await self.zhihu_client.update_cookies(browser_context=self.browser_context)

            crawler_type_var.set(context.crawler_type)
            if context.crawler_type == "search":
                await self.search(context)
            elif context.crawler_type == "detail":
                await self.get_specified_contents(context)
            elif context.crawler_type == "creator":
                await self.get_creators_and_contents(context)

        utils.logger.info("[ZhihuCrawler.start] Zhihu Crawler finished ...")

    async def search(self, context: CrawlerContext) -> None:
        utils.logger.info("[ZhihuCrawler.search] Begin search zhihu keywords")
        zhihu_limit_count = 20  # zhihu limit page fixed value
        if context.crawler_max_notes_count < zhihu_limit_count:
            context.crawler_max_notes_count = zhihu_limit_count
        start_page = context.start_page
        for keyword in context.keywords.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[ZhihuCrawler.search] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * zhihu_limit_count <= context.crawler_max_notes_count:
                if page < start_page:
                    utils.logger.info(f"[ZhihuCrawler.search] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(f"[ZhihuCrawler.search] search zhihu keyword: {keyword}, page: {page}")
                    content_list: List[ZhihuContent]  = await self.zhihu_client.get_note_by_keyword(
                        keyword=keyword,
                        page=page,
                    )
                    utils.logger.info(f"[ZhihuCrawler.search] Search contents :{content_list}")
                    if not content_list:
                        utils.logger.info("No more content!")
                        break

                    page += 1
                    for content in content_list:
                        await zhihu_store.update_zhihu_content(content)

                    await self.batch_get_content_comments(context, content_list)
                except DataFetchError:
                    utils.logger.error("[ZhihuCrawler.search] Search content error")
                    return

    async def batch_get_content_comments(self, context: CrawlerContext, content_list: List[ZhihuContent]):
        if not context.enable_get_comments:
            utils.logger.info(f"[ZhihuCrawler.batch_get_content_comments] Crawling comment mode is not enabled")
            return

        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        task_list: List[Task] = []
        for content_item in content_list:
            task = asyncio.create_task(self.get_comments(context, content_item, semaphore), name=content_item.content_id)
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_comments(self, context: CrawlerContext, content_item: ZhihuContent, semaphore: asyncio.Semaphore):
        async with semaphore:
            utils.logger.info(f"[ZhihuCrawler.get_comments] Begin get note id comments {content_item.content_id}")
            await self.zhihu_client.get_note_all_comments(
                content=content_item,
                callback=zhihu_store.batch_update_zhihu_content_comments
            )

    async def get_specified_contents(self, context: CrawlerContext):
        utils.logger.info("[ZhihuCrawler.get_specified_contents] Begin get specified zhihu contents")
        for content_id in context.zhihu_specified_id_list:
            content_item = await self.zhihu_client.get_content_info(content_id)
            if content_item:
                await zhihu_store.update_zhihu_content(content_item)
                await self.batch_get_content_comments(context, [content_item])

    async def get_creators_and_contents(self, context: CrawlerContext):
        utils.logger.info("[ZhihuCrawler.get_creators_and_contents] Begin get zhihu creators")
        for user_id in context.zhihu_creator_id_list:
            creator_info: Dict = await self.zhihu_client.get_user_info(user_id)
            if creator_info:
                await zhihu_store.save_creator(user_id, creator=creator_info)

            all_content_list = await self.zhihu_client.get_all_user_contents(
                user_id=user_id,
                callback=self.fetch_creator_content_detail
            )

            content_ids = [content.content_id for content in all_content_list]
            await self.batch_get_content_comments(context, all_content_list)

    async def fetch_creator_content_detail(self, content_list: List[ZhihuContent]):
        semaphore = asyncio.Semaphore(self.context.max_concurrency_num)
        task_list = [
            self.get_content_detail_async_task(content.content_id, semaphore) for content in content_list
        ]

        content_details = await asyncio.gather(*task_list)
        for content_detail in content_details:
            if content_detail is not None:
                await zhihu_store.update_zhihu_content(content_detail)

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
        utils.logger.info("[ZhihuCrawler.launch_browser] Begin create browser context ...")
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

    async def create_zhihu_client(self, context: CrawlerContext, httpx_proxy: Optional[str]) -> ZhiHuClient:
        utils.logger.info("[ZhihuCrawler.create_zhihu_client] Begin create zhihu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        zhihu_client_obj = ZhiHuClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Origin": "https://www.zhihu.com",
                "Referer": "https://www.zhihu.com",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return zhihu_client_obj
