import sqlite3
from mysql.connector import pooling
from typing import Dict, List
from log import get_logger
from abc import ABC, abstractmethod

logger = get_logger()

class BaseDbManager(ABC):
    """数据库管理基类"""
    def __init__(self, config: Dict):
        self.config = config

    @abstractmethod
    def get_connection(self):
        """获取数据库连接"""
        raise NotImplementedError

    @abstractmethod
    def execute(self, sql: str, params: tuple = None):
        """执行单条 SQL 语句"""
        raise NotImplementedError

    @abstractmethod
    def execute_many(self, sql: str, params_list: List[tuple]):
        """执行批量SQL语句"""
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """关闭数据库连接"""
        raise NotImplementedError

    def __del__(self):
        """析构函数，确保连接池被正确关闭"""
        try:
            self.close()
        except Exception as e:
            logger.error(f"析构时关闭数据库连接池失败: {e}")

class MysqlDbManager(BaseDbManager):
    """数据库管理类"""
    def __init__(self, config: Dict):
        super().__init__(config)
        self._create_pool()
        self.create_tables()

    def _create_pool(self):
        """创建数据库连接池"""
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="mypool",
                pool_size=2,
                **self.config
            )
            logger.info("数据库连接池创建成功")
        except Exception as e:
            logger.error(f"创建数据库连接池失败: {e}")
            raise

    def get_connection(self):
        """获取数据库连接"""
        try:
            return self.pool.get_connection()
        except Exception as e:
            logger.error(f"获取数据库连接失败: {e}")
            raise

    def execute(self, sql: str, params: tuple = None):
        """执行单条 SQL 语句"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
            conn.commit()

    def execute_many(self, sql: str, params_list: List[tuple]):
        """执行批量SQL语句"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, params_list)
            conn.commit()

    def _create_asset_history(self):
        """创建必要的数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS assets_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            created_at DATETIME NOT NULL,
            coin VARCHAR(20) NOT NULL,
            exchange VARCHAR(50) NOT NULL,
            type VARCHAR(20) NOT NULL,
            free DECIMAL(30,8) NOT NULL,
            locked DECIMAL(30,8) NOT NULL,
            total DECIMAL(30,8) NOT NULL,
            price_usdt DECIMAL(30,8) NOT NULL,
            total_usdt DECIMAL(30,8) NOT NULL,
            
            INDEX idx_user_time (user_id, created_at),
            INDEX idx_exchange (exchange)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        try:
            self.execute(create_table_sql)
            logger.info("数据表创建成功")
        except Exception as e:
            logger.error(f"创建数据表失败: {e}")
            raise
        
    def _create_total_assets_history(self):
        """创建必要的数据表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS total_assets_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            created_at DATETIME NOT NULL,
            total_usdt DECIMAL(30,8) NOT NULL,
            detail TEXT NOT NULL,
            
            INDEX idx_user_time (user_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        try:
            self.execute(create_table_sql)
            logger.info("数据表创建成功")
        except Exception as e:
            logger.error(f"创建数据表失败: {e}")
            raise
        
    def create_tables(self):
        self._create_asset_history()
        self._create_total_assets_history()

    def close(self):
        """关闭数据库连接池"""
        try:
            if hasattr(self, 'pool'):
                # 关闭连接池
                self.pool._remove_connections()
                logger.info("数据库连接池已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接池失败: {e}")
            raise

    def __del__(self):
        """析构函数，确保连接池被正确关闭"""
        try:
            self.close()
        except Exception as e:
            logger.error(f"析构时关闭数据库连接池失败: {e}")

class SQLiteDbManager(BaseDbManager):
    """SQLite 数据库管理类"""
    def __init__(self, config: Dict):
        super().__init__(config)
        self.connection = sqlite3.connect(config['database'], check_same_thread=False)
        self.create_tables()

    def get_connection(self):
        """获取数据库连接"""
        return self.connection

    def execute(self, sql: str, params: tuple = None):
        """执行单条 SQL 语句"""
        cursor = self.get_connection().cursor()
        cursor.execute(sql)
        self.get_connection().commit()
        cursor.close()

    def execute_many(self, sql: str, params_list: List[tuple]):
        """执行批量SQL语句"""
        cursor = self.get_connection().cursor()
        cursor.executemany(sql, params_list)
        self.get_connection().commit()
        cursor.close()

    def create_tables(self):
        """创建必要的数据表"""
        self._create_asset_history()
        self._create_total_assets_history()

    def _create_asset_history(self):
        """创建资产历史表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS assets_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            created_at DATETIME NOT NULL,
            coin TEXT NOT NULL,
            exchange TEXT NOT NULL,
            type TEXT NOT NULL,
            free REAL NOT NULL,
            locked REAL NOT NULL,
            total REAL NOT NULL,
            price_usdt REAL NOT NULL,
            total_usdt REAL NOT NULL
        );
        """
        try:
            self.execute(create_table_sql)
            logger.info("资产历史表创建成功")
        except Exception as e:
            logger.error(f"创建资产历史表失败: {e}")
            raise

    def _create_total_assets_history(self):
        """创建总资产历史表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS total_assets_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            created_at DATETIME NOT NULL,
            total_usdt REAL NOT NULL,
            detail TEXT NOT NULL
        );
        """
        try:
            self.execute(create_table_sql)
            logger.info("总资产历史表创建成功")
        except Exception as e:
            logger.error(f"创建总资产历史表失败: {e}")
            raise

    def close(self):
        """关闭数据库连接"""
        try:
            if hasattr(self, 'connection'):
                self.connection.close()  # 关闭 SQLite 连接
                logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")
            raise

    def __del__(self):
        """析构函数，确保连接被正确关闭"""
        self.close()
