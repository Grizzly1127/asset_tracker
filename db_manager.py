from mysql.connector import pooling
from typing import Dict, List
from log import get_logger

logger = get_logger()

class DatabaseManager:
    """数据库管理类"""
    def __init__(self, config: Dict):
        self.config = config
        self._create_pool()
        self.create_tables()

    def _create_pool(self):
        """创建数据库连接池"""
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="mypool",
                pool_size=5,
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
        """执行单条SQL语句"""
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
