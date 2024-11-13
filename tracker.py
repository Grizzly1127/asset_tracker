import json

from decimal import Decimal
from typing import Dict, List
from datetime import datetime
from threading import Lock
from threading import Thread, Event

from db_manager import MysqlDbManager, SQLiteDbManager
from exchange_tracker import BaseTracker, BinanceTracker, CoinexTracker

from log import get_logger

logger = get_logger()

# 自定义 JSON 编码器
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)  # 将 Decimal 转换为字符串
        return super().default(obj)

class Tracker:
    def __init__(self, exchange_config: List[Dict], db_config: Dict, connector: str, interval: int):
        self.stop_event = Event()
        self.interval = interval
        self.exchange_config = exchange_config
        self.db_connector = connector
        self.db_config = db_config
        self.user_id = 1
        self.tickers_lock = Lock()
        self.tickers: Dict[str, Decimal] = {}
        self.trackers: Dict[str, BaseTracker] = {}
        self.threads: Dict[str, Thread] = {}

        self._init_db_manager()
        self._init_trackers()
        self._init_tickers()
    
    def _init_db_manager(self):
        """初始化数据库管理器"""
        # mysql or sqlite
        if self.db_connector == 'mysql':
            self.db_manager = MysqlDbManager(self.db_config)
        else:
            self.db_manager = SQLiteDbManager(self.db_config)

    def _init_trackers(self):
        """初始化追踪器"""
        [self._add_tracker(item) for item in self.exchange_config]

    def _init_tickers(self):
        """初始化交易对价格"""
        try:
            for exchange, tracker in self.trackers.items():
                try:
                    logger.info(f"获取 {exchange} 账户ticker")
                    exchange_tickers = tracker.get_tickers()
                    if exchange_tickers:
                        self.tickers.update(exchange_tickers)
                except Exception as e:
                    logger.error(f"获取 {exchange} ticker失败: {e}")
                    continue

        except Exception as e:
            logger.error(f"初始化ticker失败: {e}")

    def _add_tracker(self, config: Dict):
        exchange = config['exchange']
        if exchange == "binance":
            self.trackers[exchange] = BinanceTracker(config)
        elif exchange == "coinex":
            self.trackers[exchange] = CoinexTracker(config)
        else:
            logger.warning(f"未知的交易所: {exchange}")

    def _dump_to_db(self, balances: List[Dict]):
        """
        将余额信息写入数据库
        Args:
            balances: 格式化后的余额列表
        """
        try:
            # 获取当前时间戳
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 计算total_usdt总和
            total_usdt_sum = Decimal('0')
            detail: Dict[str, Decimal] = {}

            tickers: Dict[str, Decimal] = {}
            with self.tickers_lock:
                tickers = self.tickers.copy()

            # 准备批量插入的数据
            values = []
            for balance in balances:
                if balance['total'] == 0:
                    continue

                price_usdt = tickers.get(balance['coin'] + 'USDT', Decimal('0'))
                if balance['coin'] == 'USDT':
                    price_usdt = Decimal('1')
                
                # 计算总资产价值
                total_usdt = price_usdt * balance['total']
                # 累加总USDT价值
                total_usdt_sum += total_usdt
                if balance['exchange'] not in detail:
                    detail[balance['exchange']] = total_usdt
                else:
                    detail[balance['exchange']] += total_usdt
                
                values.append((
                    self.user_id,
                    current_time,
                    balance['coin'],
                    balance['exchange'],
                    balance['type'],
                    str(balance['free']),
                    str(balance['locked']),
                    str(balance['total']),
                    str(price_usdt),
                    str(total_usdt)
                ))
            
            # SQL插入语句
            sql = """
                INSERT INTO assets_history (
                    user_id, created_at, coin, exchange, type, 
                    free, locked, total, price_usdt, total_usdt
                ) VALUES 
            """
            if self.db_connector == 'mysql':
                sql += "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            else:
                sql += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            
            # 执行批量插入
            self.db_manager.execute_many(sql, values)

            # 插入总资产记录
            sql = """
                INSERT INTO total_assets_history (
                    user_id, created_at, total_usdt, detail
                ) VALUES 
            """
            if self.db_connector == 'mysql':
                sql += "(%s, %s, %s, %s)"
            else:
                sql += "(?, ?, ?, ?)"
            self.db_manager.execute_many(sql, [(
                self.user_id,
                current_time,
                str(total_usdt_sum),
                json.dumps(detail, cls=DecimalEncoder)
            )])
                
            logger.info(f"成功保存 {len(balances) + 1} 条余额记录到数据库")
            
        except Exception as e:
            logger.error(f"保存余额记录失败: {e}")
            raise

    def _tracker_account_loop(self):
        """account循环"""
        while not self.stop_event.is_set():
            try:
                try:
                    balances = []
                    for exchange, tracker in self.trackers.items():
                        logger.info(f"获取 {exchange} 账户余额")
                        balances = balances + tracker.get_account_assets()
                    if balances:
                        self._dump_to_db(balances)
                except Exception as e:
                    logger.error(f"获取 {tracker.exchange} 账户余额失败: {e}")

                # 按配置的间隔时间等待
                self.stop_event.wait(self.interval)
            except Exception as e:
                logger.error(f"余额检查循环出错: {e}")
                self.stop_event.wait(self.interval)

    def _tracker_ticker_loop(self):
        """ticker循环"""
        while not self.stop_event.is_set():
            try:
                temp_tickers = {}
                for exchange, tracker in self.trackers.items():
                    try:
                        logger.info(f"获取 {exchange} 账户ticker")
                        exchange_tickers = tracker.get_tickers()
                        if exchange_tickers:
                            temp_tickers.update(exchange_tickers)
                    except Exception as e:
                        logger.error(f"获取 {exchange} ticker失败: {e}")
                        continue
                
                if temp_tickers:
                    with self.tickers_lock:
                        self.tickers.update(temp_tickers)
                # 按配置的间隔时间等待
                self.stop_event.wait(self.interval)
            except Exception as e:
                logger.error(f"ticker检查循环出错: {e}")
                self.stop_event.wait(self.interval)

    def start(self):
        """开始追踪"""
        logger.info(f"开始追踪所有交易对最新价格")
        ticker_thread = Thread(target=self._tracker_ticker_loop, daemon=True)
        ticker_thread.start()
        self.threads["ticker"] = ticker_thread

        logger.info(f"开始追踪所有账户余额")
        account_thread = Thread(target=self._tracker_account_loop, daemon=True)
        account_thread.start()
        self.threads["account"] = account_thread


    def stop(self):
        """停止追踪"""
        self.stop_event.set()
        for exchange, thread in self.threads.items():
            logger.info(f"停止追踪 {exchange} 账户余额")
            thread.join()
