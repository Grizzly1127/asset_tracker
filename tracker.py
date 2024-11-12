import time
import json
from decimal import Decimal
from binance.client import Client as BinanceClient
from binance.cm_futures import CMFutures as BinanceFuturesClient
from binance.exceptions import BinanceAPIException
from coinex import Client as CoinexClient, CoinexAPIException
from db_manager import MysqlDbManager, SQLiteDbManager
from threading import Thread, Event
from typing import Dict, List, Callable
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock
from log import get_logger

logger = get_logger()

# 自定义 JSON 编码器
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)  # 将 Decimal 转换为字符串
        return super().default(obj)

class BaseTracker(ABC):
    ACCOUNT_TYPE_SPOT = "spot"
    ACCOUNT_TYPE_FUTURES = "futures"

    """余额追踪器基类"""
    def __init__(self, exchange: str, api_key: str, api_secret: str):
        self.exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret

    @abstractmethod
    def get_account_assets(self) -> List[Dict]:
        """获取账户余额"""
        raise NotImplementedError
    
    @abstractmethod
    def get_tickers(self) -> Dict[str, Decimal]:
        """获取交易对的最新价格"""
        raise NotImplementedError
    
    def format_balance(self, raw_balances: List[Dict], format_func: Callable[[Dict], Dict]) -> List[Dict]:
        """
        统一格式化不同交易所的余额数据
        标准格式: {
            'coin': str,           # 币种名称
            'free': Decimal,       # 可用余额
            'locked': Decimal,     # 冻结余额
            'total': Decimal,      # 总余额
            'exchange': str,       # 交易所名称
            'price_usdt': Decimal, # 币种价格
            'total_usdt': Decimal, # 总资产价值
            'type': str            # 账户类型: spot/futures
        }
        """
        formatted_balances = []
        for balance in raw_balances:
            if self._is_valid_balance(balance, format_func):
                formatted_balance = format_func(balance)
                formatted_balances.append(formatted_balance)
        return formatted_balances

    def _is_valid_balance(self, balance: Dict, format_func: Callable[[Dict], Dict]) -> bool:
        """检查余额是否有效（非零）"""
        try:
            formatted = format_func(balance)
            return formatted['total'] > Decimal('0')
        except (KeyError, ValueError, TypeError):
            return False

class BinanceTracker(BaseTracker):
    """Binance余额追踪器"""
    def __init__(self, exchange: str, api_key: str, api_secret: str):
        super().__init__(exchange, api_key, api_secret)
        self.client = BinanceClient(api_key, api_secret)
        self.futures_client = BinanceFuturesClient(api_key, api_secret)

    def _format_spot_balance(self, balance: Dict) -> Dict:
        """格式化币币账户余额数据"""
        return {
            'coin': balance['asset'],
            'free': Decimal(str(balance.get('free', '0'))),
            'locked': Decimal(str(balance.get('locked', '0'))),
            'total': Decimal(str(balance.get('free', '0'))) + Decimal(str(balance.get('locked', '0'))),
            'exchange': self.exchange,
            'type': self.ACCOUNT_TYPE_SPOT
        }

    def _format_futures_balance(self, balance: Dict) -> Dict:
        """格式化合约账户余额数据"""
        return {
            'coin': balance['asset'],
            'free': Decimal(str(balance.get('availableBalance', '0'))),
            'locked': Decimal(str(balance.get('balance', '0'))) - Decimal(str(balance.get('availableBalance', '0'))),
            'total': Decimal(str(balance.get('balance', '0'))),
            'exchange': self.exchange,
            'type': self.ACCOUNT_TYPE_FUTURES
        }

    def get_account_assets(self) -> List[Dict]:
        """获取账户总资产"""
        try:
            timestamp = int(time.time() * 1000)
            spot_response = self.client.get_account(**{'timestamp': timestamp})
            spot_balances = self.format_balance(
                [
                    balance for balance in spot_response['balances']
                    if float(balance['free']) > 0 or float(balance['locked']) > 0
                ],
                self._format_spot_balance
            )
            futures_response = self.futures_client.balance(**{'timestamp': timestamp})
            futures_balances = self.format_balance(
                [
                    balance for balance in futures_response
                    if float(balance['balance']) > 0
                ],
                self._format_futures_balance
            )
            return spot_balances + futures_balances
        except BinanceAPIException as e:
            logger.error(f"获取账户余额失败: {e}")
            return []
        
    def get_tickers(self) -> Dict[str, Decimal]:
        """获取交易对的最新价格"""
        try:
            response = self.client.get_ticker(**{'type': 'MINI'})
            return {
                item['symbol']: Decimal(item['lastPrice'])
                for item in response
            }
        except BinanceAPIException as e:
            logger.error(f"获取交易对最新价格失败: {e}")
            return {}
    

class CoinexTracker(BaseTracker):
    """Coinex余额追踪器"""
    def __init__(self, exchange: str, api_key: str, api_secret: str):
        super().__init__(exchange, api_key, api_secret)
        self.client = CoinexClient(api_key, api_secret)

    def _format_spot_balance(self, balance: Dict) -> Dict:
        """格式化Coinex的币币账户余额数据"""
        return {
            'coin': balance['ccy'],
            'free': Decimal(str(balance.get('available', '0'))),
            'locked': Decimal(str(balance.get('frozen', '0'))),
            'total': Decimal(str(balance.get('available', '0'))) + Decimal(str(balance.get('frozen', '0'))),
            'exchange': self.exchange,
            'type': self.ACCOUNT_TYPE_SPOT
        }

    def _format_futures_balance(self, balance: Dict) -> Dict:
        """格式化Coinex的币币账户余额数据"""
        available = Decimal(str(balance.get('available', '0')))
        margin = Decimal(str(balance.get('margin', '0')))
        frozen = Decimal(str(balance.get('frozen', '0')))
        unrealized_pnl = Decimal(str(balance.get('unrealized_pnl', '0')))
        return {
            'coin': balance['ccy'],
            'free': available,
            'locked': frozen,
            'total': available + margin + frozen + unrealized_pnl,
            'exchange': self.exchange,
            'type': self.ACCOUNT_TYPE_FUTURES
        }

    def get_account_assets(self) -> List[Dict]:
        """获取账户余额"""
        try:
            # 获取现货账户余额
            spot_response = self.client.get_spot_balance()
            spot_balances = self.format_balance(
                spot_response["data"],
                self._format_spot_balance
            )
            # 获取合约账户余额
            futures_response = self.client.get_futures_balance()
            futures_balances = self.format_balance(
                futures_response["data"], 
                self._format_futures_balance
            )

            return spot_balances + futures_balances
        except CoinexAPIException as e:
            logger.error(f"获取账户余额失败: {e}")
            return []
    
    def get_tickers(self) -> Dict[str, Decimal]:
        """获取交易对的最新价格"""
        try:
            response = self.client.get_spot_ticker()
            return {
                item['market']: Decimal(item['last'])
                for item in response['data']
            }
        except CoinexAPIException as e:
            logger.error(f"获取交易对最新价格失败: {e}")
            return {}

class Tracker:
    def __init__(self, exchange_config: List[Dict], db_config: Dict, interval: int):
        self.stop_event = Event()
        self.interval = interval
        self.exchange_config = exchange_config
        self.db_config = db_config
        self.user_id = 1
        self.tickers_lock = Lock()
        self.tickers: Dict[str, Decimal] = {}
        self.trackers: Dict[str, BaseTracker] = {}
        self.threads: Dict[str, Thread] = {}

        self._init_db_manager()
        self._init_trackers(exchange_config)
        self._init_tickers()
    
    def _init_db_manager(self):
        """初始化数据库管理器"""
        # mysql or sqlite
        if self.db_config.get('connector') == 'mysql':
            self.db_manager = MysqlDbManager(self.db_config)
        else:
            self.db_manager = SQLiteDbManager(self.db_config)

    def _init_trackers(self, config: List[Dict]):
        """初始化追踪器"""
        [self._add_tracker(item["exchange"].lower(), item["api_key"], item["api_secret"]) for item in config]

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

    def _add_tracker(self, exchange: str, api_key: str, api_secret: str):
        if exchange == "binance":
            self.trackers[exchange] = BinanceTracker(exchange, api_key, api_secret)
        elif exchange == "coinex":
            self.trackers[exchange] = CoinexTracker(exchange, api_key, api_secret)
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
            if self.db_config.get('connector') == 'mysql':
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
            if self.db_config.get('connector') == 'mysql':
                sql = "(%s, %s, %s, %s)"
            else:
                sql = "(?, ?, ?, ?)"
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
