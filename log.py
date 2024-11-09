import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict

def get_logger(config: Dict = None) -> None:
    """
    设置日志配置
    :param config: 日志配置字典
    """
    # 创建根日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if config:
        log_config = config['logging']
        # 清除现有的处理器
        logger.handlers.clear()
        
        # 创建格式化器
        formatter = logging.Formatter(log_config['format'])
        
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
        logging.info("日志系统初始化完成")
    return logger

# 导出logger
__all__ = ['get_logger']