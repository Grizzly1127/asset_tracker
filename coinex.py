import hashlib
import json
import time
import hmac
from urllib.parse import urlparse, urlencode
from typing import Dict, Optional, List, Tuple, Union, Any
import requests


class CoinexAPIException(Exception):
    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = "Invalid JSON error message from Coinex: {}".format(
                response.text
            )
        else:
            self.code = json_res.get("code")
            self.message = json_res.get("msg")
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, "request", None)

    def __str__(self):  # pragma: no cover
        return "APIError(code=%s): %s" % (self.code, self.message)

class CoinexRequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return "CoinexRequestException: %s" % self.message



class BaseClient:
    API_URL = "https://api.coinex.com/"
    PUBLIC_API_VERSION = "v2"
    REQUEST_TIMEOUT: float = 10

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        """Coinex API Client constructor.
        Args:
            api_key (str, optional): Coinex API key. Defaults to None.
            api_secret (str, optional): Coinex API secret. Defaults to None.
        """
        self.API_KEY = api_key
        self.API_SECRET = api_secret

    def _get_headers(self, timestamp: str, sign: str = "") -> Dict[str, str]:
        if sign == "":
            return {
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
                "X-COINEX-TIMESTAMP": timestamp,
            }
        
        return {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "X-COINEX-KEY": self.API_KEY,
            "X-COINEX-SIGN": sign,
            "X-COINEX-TIMESTAMP": timestamp,
        }
    
    def _generate_sign(self, method: str, path: str, timestamp: str, **kwargs) -> str:
        body = "" 
        if method == "GET" and len(kwargs) > 0:
            body = "?" + urlencode(kwargs)
        elif method == "POST":
            body = json.dumps(kwargs)
        prepared_str = f"{method}/{self.PUBLIC_API_VERSION}{path}{body}{timestamp}"
        signature = hmac.new(
            bytes(self.API_SECRET, 'latin-1'), 
            msg=bytes(prepared_str, 'latin-1'), 
            digestmod=hashlib.sha256
        ).hexdigest().lower()
        return signature
    
    def _create_api_uri(self, path: str) -> str:
        # https://api.coinex.com/ + v2 + /ping
        return self.API_URL + self.PUBLIC_API_VERSION + path

class Client(BaseClient):
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        super().__init__(api_key, api_secret)


    def _request(self, method, path: str, signed: bool = False, **kwargs):
        timestamp = str(int(time.time() * 1000))
        sign = ""
        if signed:
            sign = self._generate_sign(method, path, timestamp, **kwargs)

        if method == "GET":
            response = requests.get(
                self._create_api_uri(path), 
                params=kwargs,
                headers=self._get_headers(timestamp, sign),
                timeout=self.REQUEST_TIMEOUT
            )
        elif method == "POST":
            response = requests.post(
                self._create_api_uri(path), 
                data=kwargs,
                headers=self._get_headers(timestamp, sign),
                timeout=self.REQUEST_TIMEOUT
            )
        return self._handle_response(response)


    @staticmethod
    def _handle_response(response: requests.Response):
        """Internal helper for handling API responses from the Coinex server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not (200 <= response.status_code < 300):
            raise CoinexAPIException(response, response.status_code, response.text)
        try:
            return response.json()
        except ValueError:
            raise CoinexRequestException("Invalid Response: %s" % response.text)
        
    def _request_api(
        self,
        method,
        path: str,
        signed: bool = False,
        **kwargs,
    ):
        return self._request(method, path, signed, **kwargs)

    def _get(self, path, signed: bool =False, **kwargs):
        return self._request_api("GET", path, signed, **kwargs)
    
    def _post(self, path, signed: bool =False, **kwargs):
        return self._request_api("POST", path, signed, **kwargs)
    
    # Exchange Endpoints
    def ping(self) -> Dict:
        return self._get("/ping")
    
    def get_spot_balance(self) -> Dict:
        return self._get("/assets/spot/balance", True)
    
    def get_futures_balance(self) -> Dict:
        return self._get("/assets/futures/balance", True)
    
    def get_margin_balance(self) -> Dict:
        return self._get("/assets/margin/balance", True)
    
    def get_financial_balance(self) -> Dict:
        return self._get("/assets/financial/balance", True)
    
    def get_amm_liquidity(self) -> Dict:
        return self._get("/assets/amm/liquidity", True)
    
    def get_spot_market(self, **params) -> Dict:
        """Query spot market.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.

        Returns: Dict: API response.

        """
        return self._get(f"/spot/market", False, **params)
    
    def get_spot_ticker(self, **params) -> Dict:
        """Query spot ticker.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.

        Returns: Dict: API response.

        """
        return self._get(f"/spot/ticker", False, **params)
    
    def get_spot_depth(self, **params) -> Dict:
        """Query spot depth.

        Args: **params (Dict, required): Query parameters.
            - market (str, required): market name.
            - limit (int, required): Number of depth data items, One of [5, 10, 20, 50].
            - interval (str, required): Merge interval.
                    One of ["0", "0.00000000001", "0.000000000001", "0.0000000001", "0.000000001", 
                    "0.00000001", "0.0000001", "0.000001", "0.00001", "0.0001", "0.001", "0.01", "0.1", 
                    "1", "10", "100", "1000"]

        Returns: Dict: API response.

        """
        return self._get(f"/spot/depth", False, **params)
    
    def get_spot_deals(self, **params) -> Dict:
        """Query spot deals.

        Args: **params (Dict, required): Query parameters.
            - market (str, required): market name.
            - limit (int, optional): Number of transaction data items, default as 100, max. value 1000.
            - last_id (int, optional): The starting point of the query TxID, 0 means to acquire from the latest record.
        
        Returns: Dict: API response.

        """
        return self._get(f"/spot/deals", False, **params)
    
    def get_kline(self, **params) -> Dict:
        """Query kline.

        Args: **params (Dict, required): Query parameters.
            - market (str, required): market name.
            - price_type (str, optional): Price type for drawing candlesticks, default as latest_price, no mark_price in spot markets.
            - limit (int, optional): Number of transaction data items, default as 100, max. value 1000.
            - period (int, required): Candlestick period. One of ["1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", 
                "6hour", "12hour", "1day", "3day", "1week"]
        
        price_type: https://docs.coinex.com/api/v2/enum#trigger_price_type
        
        Returns: Dict: API response.

        """
        return self._get(f"/spot/kline", False, **params)

    def get_index(self, **params) -> Dict:
        """Query index.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.
        
        Returns: Dict: API response.

        """
        return self._get(f"/spot/index", False, **params)
    
    def get_future_market(self, **params) -> Dict:
        """Query future market.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.

        Returns: Dict: API response.

        """
        return self._get(f"/futures/market", False, **params)
    
    def get_future_ticker(self, **params) -> Dict:
        """Query future ticker.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.

        Returns: Dict: API response.

        """
        return self._get(f"/futures/ticker", False, **params)
    
    def get_future_depth(self, **params) -> Dict:
        """Query future depth.

        Args: **params (Dict, required): Query parameters.
            - market (str, required): market name.
            - limit (int, required): Number of depth data items, One of [5, 10, 20, 50].
            - interval (str, required): Merge interval.
                    One of ["0", "0.00000000001", "0.000000000001", "0.0000000001", "0.000000001", 
                    "0.00000001", "0.0000001", "0.000001", "0.00001", "0.0001", "0.001", "0.01", "0.1", 
                    "1", "10", "100", "1000"]

        Returns: Dict: API response.

        """
        return self._get(f"/futures/depth", False, **params)
    
    def get_future_deals(self, **params) -> Dict:
        """Query future deals.

        Args: **params (Dict, required): Query parameters.
            - market (str, required): market name.
            - limit (int, optional): Number of transaction data items, default as 100, max. value 1000.
            - last_id (int, optional): The starting point of the query TxID, 0 means to acquire from the latest record.
        
        Returns: Dict: API response.

        """
        return self._get(f"/futures/deals", False, **params)
    
    def get_future_kline(self, **params) -> Dict:
        """Query future kline.

        Args: **params (Dict, required): Query parameters.
            - market (str, required): market name.
            - price_type (str, optional): Price type for drawing candlesticks, default as latest_price, no mark_price in spot markets.
            - limit (int, optional): Number of transaction data items, default as 100, max. value 1000.
            - period (int, required): Candlestick period. One of ["1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", 
                "6hour", "12hour", "1day", "3day", "1week"]
        
        price_type: https://docs.coinex.com/api/v2/enum#trigger_price_type
        
        Returns: Dict: API response.

        """
        return self._get(f"/futures/kline", False, **params)
    
    def get_future_index(self, **params) -> Dict:
        """Query future index.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.
        
        Returns: Dict: API response.

        """
        return self._get(f"/futures/index", False, **params)
    
    def get_future_funding_rate(self, **params) -> Dict:
        """Query future funding rate.

        Args: **params (Dict, optional): Query parameters.
            - market (str, optional): List of market names. Use "," to separate multiple market names, 
                a maximum of 10 markets are allowed. An empty string or pass to query all markets.

        Returns: Dict: API response.

        """
        return self._get(f"/futures/funding_rate", False, **params)


    