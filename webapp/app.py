import asyncio
from flask import Blueprint, jsonify, request, render_template
import os
import json
import threading
import queue
from config.context import CrawlerContext
from main import CrawlerFactory
import shutil
from media_platform.xhs.core import XiaoHongShuCrawler
import logging  # 添加这行来使用 Python 的内置日志模块
import concurrent.futures

webapp_blueprint = Blueprint('webapp', __name__)

crawler_task = None
crawler_instance = None
crawler_loop = None
login_obj = None

log_queue = queue.Queue()
process = None

@webapp_blueprint.route('/')
def index():
    return render_template('index.html')

@webapp_blueprint.route('/run', methods=['POST'])
def run():
    global crawler_task, crawler_instance, crawler_loop, login_obj
    config = request.json
    context = CrawlerContext.from_dict(config)
    
    # 强制使用有头模式
    context.headless = False
    
    if context.platform == 'xhs':
        crawler_instance = XiaoHongShuCrawler()
    # 为其他平台添加相应的实例化代码
    
    if crawler_instance:
        crawler_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(crawler_loop)
        
        async def run_crawler():
            global login_obj  # 将 nonlocal 改为 global
            try:
                login_obj = await crawler_instance.start(context)
            except Exception as e:
                logging.error(f"Crawler error: {str(e)}")  # 使用 logging 模块替代 utils.logger
            finally:
                crawler_loop.stop()

        crawler_task = crawler_loop.create_task(run_crawler())
        
        def run_loop_in_thread():
            crawler_loop.run_until_complete(crawler_task)
            crawler_loop.close()

        threading.Thread(target=run_loop_in_thread, daemon=True).start()
        return jsonify({"status": "success", "message": "Crawler started in headed mode"})
    else:
        return jsonify({"status": "error", "message": "Invalid platform"})

@webapp_blueprint.route('/stop', methods=['GET'])
def stop():
    global crawler_task, crawler_instance, crawler_loop, login_obj
    if crawler_task and crawler_loop:
        def stop_crawler():
            try:
                # 取消主任务
                if not crawler_task.done():
                    crawler_task.cancel()
                
                # 停止爬虫实例
                if crawler_instance and hasattr(crawler_instance, 'stop'):
                    future = asyncio.run_coroutine_threadsafe(crawler_instance.stop(), crawler_loop)
                    future.result(timeout=10)
                
                # 停止事件循环
                crawler_loop.call_soon_threadsafe(crawler_loop.stop)
            except Exception as e:
                logging.error(f"Error during stop operation: {str(e)}")

        # 在新的线程中执行停止操作
        stop_thread = threading.Thread(target=stop_crawler)
        stop_thread.start()
        stop_thread.join(timeout=15)  # 等待停止操作完成，最多等待15秒

        crawler_task = None
        crawler_instance = None
        crawler_loop = None
        login_obj = None
        return jsonify({"status": "success", "message": "Crawler stopped"})
    return jsonify({"status": "error", "message": "No crawler running"})

@webapp_blueprint.route('/logs', methods=['GET'])
def logs():
    # 实现获取日志的逻辑
    return jsonify({"logs": "日志内容"})

@webapp_blueprint.route('/save_config', methods=['POST'])
def save_config():
    config = request.json
    with open('config/web_config.json', 'w') as config_file:
        json.dump(config, config_file)
    return jsonify({"status": "success"})

@webapp_blueprint.route('/clear_browser_data', methods=['POST'])
def clear_browser_data():
    browser_data_dir = os.path.join(os.getcwd(), "browser_data")
    if os.path.exists(browser_data_dir):
        try:
            shutil.rmtree(browser_data_dir)
            return jsonify({"status": "success", "message": "浏览器数据已清理"})
        except Exception as e:
            return jsonify({"status": "error", "message": f"清理浏览器数据失败: {str(e)}"})
    else:
        return jsonify({"status": "success", "message": "浏览器数据目录不存在，无需清理"})