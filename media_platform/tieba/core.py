import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

from base.base_crawler import AbstractCrawler
from config.context import CrawlerContext
from model.m_baidu_tieba import TiebaCreator, TiebaNote
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import tieba as tieba_store
from tools import utils
from tools.crawler_util import format_proxy_info
from var import crawler_type_var, source_keyword_var

from .client import BaiduTieBaClient
from .field import SearchNoteType, SearchSortType
from .login import BaiduTieBaLogin


class TieBaCrawler(AbstractCrawler):
    context_page: Page
    tieba_client: BaiduTieBaClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://tieba.baidu.com"
        self.user_agent = utils.get_user_agent()

    async def start(self, context: CrawlerContext) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None
        if context.enable_ip_proxy:
            ip_proxy_pool = await create_ip_pool(context.ip_proxy_pool_count, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = format_proxy_info(ip_proxy_info)

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
            await self.context_page.goto(self.index_url)

            self.tieba_client = await self.create_tieba_client(context, httpx_proxy_format)
            if not await self.tieba_client.pong():
                login_obj = BaiduTieBaLogin(
                    login_type=context.login_type,
                    login_phone="",  # your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=context.cookies
                )
                await login_obj.begin()
                await self.tieba_client.update_cookies(browser_context=self.browser_context)

            crawler_type_var.set(context.crawler_type)
            if context.crawler_type == "search":
                await self.search(context)
                await self.get_specified_tieba_notes(context)
            elif context.crawler_type == "detail":
                await self.get_specified_notes(context)
            elif context.crawler_type == "creator":
                await self.get_creators_and_notes(context)

        utils.logger.info("[BaiduTieBaCrawler.start] Tieba Crawler finished ...")

    async def search(self, context: CrawlerContext) -> None:
        utils.logger.info("[BaiduTieBaCrawler.search] Begin search baidu tieba keywords")
        tieba_limit_count = 10  # tieba limit page fixed value
        if context.crawler_max_notes_count < tieba_limit_count:
            context.crawler_max_notes_count = tieba_limit_count
        start_page = context.start_page
        for keyword in context.keywords.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[BaiduTieBaCrawler.search] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * tieba_limit_count <= context.crawler_max_notes_count:
                if page < start_page:
                    utils.logger.info(f"[BaiduTieBaCrawler.search] Skip page {page}")
                    page += 1
                    continue
                try:
                    utils.logger.info(f"[BaiduTieBaCrawler.search] search tieba keyword: {keyword}, page: {page}")
                    notes_list: List[TiebaNote] = await self.tieba_client.get_notes_by_keyword(
                        keyword=keyword,
                        page=page,
                        page_size=tieba_limit_count,
                        sort=SearchSortType.TIME_DESC,
                        note_type=SearchNoteType.FIXED_THREAD
                    )
                    if not notes_list:
                        utils.logger.info(f"[BaiduTieBaCrawler.search] Search note list is empty")
                        break
                    utils.logger.info(f"[BaiduTieBaCrawler.search] Note list len: {len(notes_list)}")
                    await self.get_specified_notes(context, note_id_list=[note_detail.note_id for note_detail in notes_list])
                    page += 1
                except Exception as ex:
                    utils.logger.error(
                        f"[BaiduTieBaCrawler.search] Search keywords error, current page: {page}, current keyword: {keyword}, err: {ex}")
                    break

    async def get_specified_tieba_notes(self, context: CrawlerContext):
        tieba_limit_count = 50
        if context.crawler_max_notes_count < tieba_limit_count:
            context.crawler_max_notes_count = tieba_limit_count
        for tieba_name in context.tieba_name_list:
            utils.logger.info(
                f"[BaiduTieBaCrawler.get_specified_tieba_notes] Begin get tieba name: {tieba_name}")
            page_number = 0
            while page_number <= context.crawler_max_notes_count:
                note_list: List[TiebaNote] = await self.tieba_client.get_notes_by_tieba_name(
                    tieba_name=tieba_name,
                    page_num=page_number
                )
                if not note_list:
                    utils.logger.info(
                        f"[BaiduTieBaCrawler.get_specified_tieba_notes] Get note list is empty")
                    break

                utils.logger.info(
                    f"[BaiduTieBaCrawler.get_specified_tieba_notes] tieba name: {tieba_name} note list len: {len(note_list)}")
                await self.get_specified_notes(context, [note.note_id for note in note_list])
                page_number += tieba_limit_count

    async def get_specified_notes(self, context: CrawlerContext, note_id_list: List[str] = None):
        if note_id_list is None:
            note_id_list = context.tieba_specified_id_list
        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        task_list = [
            self.get_note_detail_async_task(note_id=note_id, semaphore=semaphore) for note_id in note_id_list
        ]
        note_details = await asyncio.gather(*task_list)
        note_details_model: List[TiebaNote] = []
        for note_detail in note_details:
            if note_detail is not None:
                note_details_model.append(note_detail)
                await tieba_store.update_tieba_note(note_detail)
        await self.batch_get_note_comments(context, note_details_model)

    async def get_note_detail_async_task(self, note_id: str, semaphore: asyncio.Semaphore) -> Optional[TiebaNote]:
        async with semaphore:
            try:
                utils.logger.info(f"[BaiduTieBaCrawler.get_note_detail_async_task] begin get note_id: {note_id} detail ...")
                note_detail: TiebaNote = await self.tieba_client.get_note_detail(note_id)
                return note_detail
            except Exception as e:
                utils.logger.error(f"[BaiduTieBaCrawler.get_note_detail_async_task] get note_id: {note_id} detail error: {e}")
                return None

    async def batch_get_note_comments(self, context: CrawlerContext, note_list: List[TiebaNote]):
        if not context.enable_get_comments:
            utils.logger.info(f"[BaiduTieBaCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[BaiduTieBaCrawler.batch_get_note_comments] note ids:{[note.note_id for note in note_list]}")
        semaphore = asyncio.Semaphore(context.max_concurrency_num)
        task_list: List[Task] = []
        for note in note_list:
            task = asyncio.create_task(self.get_comments(context, note, semaphore), name=note.note_id)
            task_list.append(task)

        await asyncio.gather(*task_list)

    async def get_comments(self, context: CrawlerContext, note: TiebaNote, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                utils.logger.info(f"[BaiduTieBaCrawler.get_comments] begin get note_id: {note.note_id} comments ...")
                await self.tieba_client.get_note_all_comments(
                    note_id=note.note_id,
                    callback=tieba_store.batch_update_tieba_note_comments
                )
            except Exception as e:
                utils.logger.error(f"[BaiduTieBaCrawler.get_comments] get note_id: {note.note_id} comments error: {e}")

    async def get_creators_and_notes(self, context: CrawlerContext):
        utils.logger.info("[BaiduTieBaCrawler.get_creators_and_notes] Begin get tieba creators")
        for user_id in context.tieba_creator_id_list:
            creator_info: TiebaCreator = await self.tieba_client.get_user_info(user_id)
            if creator_info:
                await tieba_store.save_creator(user_id, creator=creator_info)

            all_note_list = await self.tieba_client.get_all_user_notes(
                user_id=user_id,
                callback=self.fetch_creator_note_detail
            )

            note_ids = [note.note_id for note in all_note_list]
            await self.batch_get_note_comments(context, all_note_list)

    async def fetch_creator_note_detail(self, note_list: List[TiebaNote]):
        semaphore = asyncio.Semaphore(self.context.max_concurrency_num)
        task_list = [
            self.get_note_detail_async_task(note.note_id, semaphore) for note in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail is not None:
                await tieba_store.update_tieba_note(note_detail)

    async def launch_browser(self, context: CrawlerContext, chromium: BrowserType, playwright_proxy: Optional[Dict], user_agent: Optional[str]) -> BrowserContext:
        utils.logger.info("[BaiduTieBaCrawler.launch_browser] Begin create browser context ...")
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

    async def create_tieba_client(self, context: CrawlerContext, httpx_proxy: Optional[str]) -> BaiduTieBaClient:
        utils.logger.info("[BaiduTieBaCrawler.create_tieba_client] Begin create tieba API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        tieba_client_obj = BaiduTieBaClient(
            ip_pool=None,
            default_ip_proxy=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Origin": "https://tieba.baidu.com",
                "Referer": "https://tieba.baidu.com",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return tieba_client_obj
