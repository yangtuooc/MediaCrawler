import asyncio
import functools
import sys
from typing import Optional

from playwright.async_api import BrowserContext, Page
from tenacity import (RetryError, retry, retry_if_result, stop_after_attempt,
                      wait_fixed, stop_after_delay)

import config
from base.base_crawler import AbstractLogin
from cache.cache_factory import CacheFactory
from tools import utils


class XiaoHongShuLogin(AbstractLogin):

    def __init__(self,
                 login_type: str,
                 browser_context: BrowserContext,
                 context_page: Page,
                 login_phone: Optional[str] = "",
                 cookie_str: str = ""
                 ):
        config.LOGIN_TYPE = login_type
        self.browser_context = browser_context
        self.context_page = context_page
        self.login_phone = login_phone
        self.cookie_str = cookie_str
        self.stop_flag = False  # 添加停止标志

    @retry(stop=(stop_after_attempt(600) | stop_after_delay(300)), 
           wait=wait_fixed(1), 
           retry=retry_if_result(lambda value: value is False))
    async def check_login_state(self, no_logged_in_session: str) -> bool:
        if self.stop_flag:
            utils.logger.info("[XiaoHongShuLogin.check_login_state] Login process stopped by user")
            return True  # 改为返回 True 以结束重试

        current_cookie = await self.browser_context.cookies()
        current_session = next((cookie for cookie in current_cookie if cookie["name"] == "web_session"), None)
        if current_session and current_session["value"] != no_logged_in_session:
            utils.logger.info("[XiaoHongShuLogin.check_login_state] Login successful")
            return True
        else:
            utils.logger.info("[XiaoHongShuLogin.check_login_state] 登录过程中出现验证码，请手动验证")
            return False

    async def begin(self):
        """Start login xiaohongshu"""
        utils.logger.info("[XiaoHongShuLogin.begin] Begin login xiaohongshu ...")
        try:
            no_logged_in_session = next((cookie for cookie in await self.browser_context.cookies() if cookie["name"] == "web_session"), None)
            if no_logged_in_session:
                no_logged_in_session = no_logged_in_session["value"]
            else:
                no_logged_in_session = ""

            if config.LOGIN_TYPE == "qrcode":
                await self.login_by_qrcode()
            elif config.LOGIN_TYPE == "phone":
                await self.login_by_mobile()
            elif config.LOGIN_TYPE == "cookie":
                await self.login_by_cookies()
            else:
                raise ValueError("[XiaoHongShuLogin.begin] Invalid Login Type. Currently only supported qrcode or phone or cookies ...")

            await self.handle_captcha()  # 处理可能出现的验证码

            login_success = await self.check_login_state(no_logged_in_session)
            if not login_success:
                utils.logger.info("登录未成功，可能需要进一步处理。")
                # 这里可以添加额外的处理逻辑，比如重试登录或者抛出异常
        except RetryError:
            if not self.stop_flag:
                utils.logger.error("[XiaoHongShuLogin.begin] Login failed after multiple attempts")
                sys.exit()

    async def stop(self):
        self.stop_flag = True
        try:
            # 关闭任何需要关闭的资源
            pass
        except Exception as e:
            utils.logger.error(f"Error during XiaoHongShuLogin stop: {str(e)}")
        utils.logger.info("[XiaoHongShuLogin.stop] Login process stopped")

    async def login_by_mobile(self):
        """Login xiaohongshu by mobile"""
        utils.logger.info("[XiaoHongShuLogin.login_by_mobile] Begin login xiaohongshu by mobile ...")
        await asyncio.sleep(1)
        try:
            # 小红书进入首页后，有可能不会自动弹出登录框，需要手动点击登录按钮
            login_button_ele = await self.context_page.wait_for_selector(
                selector="xpath=//*[@id='app']/div[1]/div[2]/div[1]/ul/div[1]/button",
                timeout=5000
            )
            await login_button_ele.click()
            # 弹窗的登录对话框也有两种形态，一种是直接可以看到手机号和验证码的
            # 另一种是需要点击切换到手机登录的
            element = await self.context_page.wait_for_selector(
                selector='xpath=//div[@class="login-container"]//div[@class="other-method"]/div[1]',
                timeout=5000
            )
            await element.click()
        except Exception as e:
            utils.logger.info("[XiaoHongShuLogin.login_by_mobile] have not found mobile button icon and keep going ...")

        await asyncio.sleep(1)
        login_container_ele = await self.context_page.wait_for_selector("div.login-container")
        input_ele = await login_container_ele.query_selector("label.phone > input")
        await input_ele.fill(self.login_phone)
        await asyncio.sleep(0.5)

        send_btn_ele = await login_container_ele.query_selector("label.auth-code > span")
        await send_btn_ele.click()  # 点击发送验证码
        sms_code_input_ele = await login_container_ele.query_selector("label.auth-code > input")
        submit_btn_ele = await login_container_ele.query_selector("div.input-container > button")
        cache_client = CacheFactory.create_cache(config.CACHE_TYPE_MEMORY)
        max_get_sms_code_time = 60 * 2  # 最长获取验证码的时间为2分钟
        no_logged_in_session = ""
        while max_get_sms_code_time > 0:
            utils.logger.info(f"[XiaoHongShuLogin.login_by_mobile] get sms code from redis remaining time {max_get_sms_code_time}s ...")
            await asyncio.sleep(1)
            sms_code_key = f"xhs_{self.login_phone}"
            sms_code_value = cache_client.get(sms_code_key)
            if not sms_code_value:
                max_get_sms_code_time -= 1
                continue

            current_cookie = await self.browser_context.cookies()
            _, cookie_dict = utils.convert_cookies(current_cookie)
            no_logged_in_session = cookie_dict.get("web_session")

            await sms_code_input_ele.fill(value=sms_code_value.decode())  # 输入短信验证码
            await asyncio.sleep(0.5)
            agree_privacy_ele = self.context_page.locator("xpath=//div[@class='agreements']//*[local-name()='svg']")
            await agree_privacy_ele.click()  # 点击同意隐私协议
            await asyncio.sleep(0.5)

            await submit_btn_ele.click()  # 点击登录

            # todo ... 应该还需要检查验证码的正确性有可能输入的验证码不正确
            break

        try:
            await self.check_login_state(no_logged_in_session)
        except RetryError:
            utils.logger.info("[XiaoHongShuLogin.login_by_mobile] Login xiaohongshu failed by mobile login method ...")
            sys.exit()

        wait_redirect_seconds = 5
        utils.logger.info(f"[XiaoHongShuLogin.login_by_mobile] Login successful then wait for {wait_redirect_seconds} seconds redirect ...")
        await asyncio.sleep(wait_redirect_seconds)

    async def login_by_qrcode(self):
        """login xiaohongshu website and keep webdriver login state"""
        utils.logger.info("[XiaoHongShuLogin.login_by_qrcode] Begin login xiaohongshu by qrcode ...")
        # login_selector = "div.login-container > div.left > div.qrcode > img"
        qrcode_img_selector = "xpath=//img[@class='qrcode-img']"
        # find login qrcode
        base64_qrcode_img = await utils.find_login_qrcode(
            self.context_page,
            selector=qrcode_img_selector
        )
        if not base64_qrcode_img:
            utils.logger.info("[XiaoHongShuLogin.login_by_qrcode] login failed , have not found qrcode please check ....")
            # if this website does not automatically popup login dialog box, we will manual click login button
            await asyncio.sleep(0.5)
            login_button_ele = self.context_page.locator("xpath=//*[@id='app']/div[1]/div[2]/div[1]/ul/div[1]/button")
            await login_button_ele.click()
            base64_qrcode_img = await utils.find_login_qrcode(
                self.context_page,
                selector=qrcode_img_selector
            )
            if not base64_qrcode_img:
                sys.exit()

        # get not logged session
        current_cookie = await self.browser_context.cookies()
        _, cookie_dict = utils.convert_cookies(current_cookie)
        no_logged_in_session = cookie_dict.get("web_session")

        # show login qrcode
        # fix issue #12
        # we need to use partial function to call show_qrcode function and run in executor
        # then current asyncio event loop will not be blocked
        partial_show_qrcode = functools.partial(utils.show_qrcode, base64_qrcode_img)
        asyncio.get_running_loop().run_in_executor(executor=None, func=partial_show_qrcode)

        utils.logger.info(f"[XiaoHongShuLogin.login_by_qrcode] waiting for scan code login, remaining time is 120s")
        try:
            await self.check_login_state(no_logged_in_session)
        except RetryError:
            utils.logger.info("[XiaoHongShuLogin.login_by_qrcode] Login xiaohongshu failed by qrcode login method ...")
            sys.exit()

        wait_redirect_seconds = 5
        utils.logger.info(f"[XiaoHongShuLogin.login_by_qrcode] Login successful then wait for {wait_redirect_seconds} seconds redirect ...")
        await asyncio.sleep(wait_redirect_seconds)

    async def login_by_cookies(self):
        """login xiaohongshu website by cookies"""
        utils.logger.info("[XiaoHongShuLogin.login_by_cookies] Begin login xiaohongshu by cookie ...")
        for key, value in utils.convert_str_cookie_to_dict(self.cookie_str).items():
            if key != "web_session":  # only set web_session cookie attr
                continue
            await self.browser_context.add_cookies([{
                'name': key,
                'value': value,
                'domain': ".xiaohongshu.com",
                'path': "/"
            }])

    async def handle_captcha(self):
        # 检测是否出现验证码
        captcha_element = await self.context_page.query_selector('.captcha-container')
        if captcha_element:
            utils.logger.info("检测到验证码，请在浏览器中手动完成验证。")
            await self.wait_for_verification_completion()

    async def wait_for_verification_completion(self):
        utils.logger.info("等待验证完成...")
        while True:
            # 检查验证码容器是否还存在
            captcha_element = await self.context_page.query_selector('.captcha-container')
            if not captcha_element:
                utils.logger.info("验证已完成，继续执行登录流程...")
                break
            await asyncio.sleep(1)  # 每秒检查一次

    async def wait_for_manual_verification(self):
        utils.logger.info("请在浏览器中完成验证，完成后在控制台输入 'done' 并按回车。")
        while True:
            user_input = await asyncio.get_event_loop().run_in_executor(None, input, "验证完成后输入 'done': ")
            if user_input.lower() == 'done':
                break
        utils.logger.info("继续执行登录流程...")

