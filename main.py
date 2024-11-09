import time
import logging
import os
import json
from logging.handlers import RotatingFileHandler
from typing import Dict
from tracker import Tracker

logger = None

class Config:
    """配置加载类"""
    @staticmethod
    def load_config(file_path: str = 'test_config.json') -> Dict:
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载配置文件失败: {e}\n")
            raise

def setup_logging(config: dict) -> None:
    """
    设置日志配置
    :param config: 日志配置字典
    """
    log_config = config['logging']
    log_level = getattr(logging, log_config['level'].upper())
    log_format = log_config['format']
    
    # 创建根日志记录器
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # 清除现有的处理器
    logger.handlers.clear()
    
    # 创建格式化器
    formatter = logging.Formatter(log_format)
    
    # 配置控制台输出
    if log_config['console']['enabled']:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # 配置文件输出
    if log_config['file']['enabled']:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_config['file']['path'])
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = RotatingFileHandler(
            filename=log_config['file']['path'],
            maxBytes=log_config['file']['max_bytes'],
            backupCount=log_config['file']['backup_count'],
            encoding='utf-8'  # 添加这一行
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    print("日志系统初始化完成")
    logging.info("日志系统初始化完成")

def main_loop():
    """主循环"""
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"主循环出错: {e}")

def run_tracker():
    """运行余额追踪器"""
    try:
        # 加载配置
        config = Config.load_config()
        
        # 初始化日志系统
        setup_logging(config)
        
        # 创建并启动追踪管理器
        manager = Tracker(config["exchange_api"], config["database"], config["schedule"]["interval_seconds"])
        manager.start()
        main_loop()
        manager.stop()
                
    except Exception as e:
        logging.error(f"程序异常退出: {e}")

if __name__ == "__main__":
    run_tracker()
