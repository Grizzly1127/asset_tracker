import time
import sys
import json
import signal
import log
from log import get_logger
from typing import Dict
from tracker import Tracker

if not sys.platform.startswith('win'):
    import daemon
    import daemon.pidfile



def load_config(file_path: str = './test_config.json') -> Dict:
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}\n")

config = load_config()
logger = get_logger(config)
manager: Tracker = None

def run_tracker():
    """运行余额追踪器"""
    try:
        # 创建并启动追踪管理器
        global manager
        manager = Tracker(config["exchange_api"], config["database"], config["db_connector"], config["schedule"]["interval_seconds"])
        manager.start()
                
    except Exception as e:
        logger.error(f"程序异常退出: {e}")

# 运行标志
running = True
def handle_sigterm(signum, frame):
    global running
    logger.info("收到关闭信号，正在关闭守护进程...")
    running = False

def is_daemon() -> bool:
    return config['daemon'] > 0

def main_loop():
    """主循环"""
    daemon_file = './daemon-log.txt'
    while running:
        try:
            if is_daemon():
                with open(daemon_file, "a") as f:
                    f.write(f"Daemon alive! {time.ctime()}\n")
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"主循环出错: {e}")

    if is_daemon():
        with open(daemon_file, "a") as f:
            f.write("Daemon is shutting down gracefully.\n")

if __name__ == "__main__":
    run_tracker()

    # 运行守护进程
    if sys.platform.startswith('win') or not is_daemon():
        main_loop()
    else:
        signal.signal(signal.SIGTERM, handle_sigterm)  # 处理 SIGTERM 信号
        signal.signal(signal.SIGINT, handle_sigterm)  # 处理 SIGINT 信号
        with daemon.DaemonContext():
            main_loop()
    
    if manager:
        manager.stop()
    
