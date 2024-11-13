import time

from abc import ABC, abstractmethod
from typing import Dict, List, Callable
from decimal import Decimal

from binance.client import Client as BinanceClient
from binance.cm_futures import CMFutures as BinanceFuturesClient
from binance.exceptions import BinanceAPIException
from coinex import Client as CoinexClient, CoinexAPIException

from log import get_logger

logger = get_logger()

class BaseTracker(ABC):
    ACCOUNT_TYPE_SPOT = "spot"
    ACCOUNT_TYPE_FUTURES = "futures"

    """余额追踪器基类"""
    def __init__(self, config: Dict):
        self.exchange = config['exchange']
        self.api_key = config['api_key']
        self.api_secret = config['api_secret']

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
    def __init__(self, config: Dict):
        super().__init__(config)
        self.client = BinanceClient(self.api_key, self.api_secret)
        self.futures_client = BinanceFuturesClient(self.api_key, self.api_secret)

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
    def __init__(self, config: Dict):
        super().__init__(config)
        self.client = CoinexClient(self.api_key, self.api_secret)

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
