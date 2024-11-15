﻿## 项目简介
asset_tracker 是一个用于跟踪和管理资产多个交易所的 Python 应用程序。

目前支持的交易所包括：

- Binance
- CoinEx

## 安装

### 数据库

数据库支持 MySQL 和 SQLite。  
可根据需要选择数据库。

请确保您已安装数据库，并创建名为 `exchanges` 的数据库。  
该服务启动默认创建 `assets_history` 表和 `total_assets_history` 表。

- `assets_history` 表用于存储每个资产的记录。
- `total_assets_history` 表用于存储每个交易所的总资产。

`assets_history` 表结构如下：  
```sql
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
```

`total_assets_history` 表结构如下：  
```sql
CREATE TABLE IF NOT EXISTS total_assets_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    created_at DATETIME NOT NULL,
    total_usdt DECIMAL(30,8) NOT NULL,
            
    INDEX idx_user_time (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### python 环境

请确保您已安装 Python 3.x，并使用以下命令安装所需的依赖项：

```bash
pip install -r requirements.txt
```

## 使用方法

### 配置

编辑并修改`config.py`文件，配置数据库连接、交易所 API 密钥等信息。  

```json
{
    "exchange_api": [
        {
            "exchange": "coinex",
            "api_key": "",
            "api_secret": ""
        },
        {
            "exchange": "binance",
            "api_key": "",
            "api_secret": ""
        }
    ],
    "db_connector": "mysql",
    "database": {
        "host": "localhost",
        "user": "root",
        "password": "123456",
        "database": "exchanges",
        "charset": "utf8mb4"
    }
}
```

数据库可选择 MySQL 或 SQLite， 只需修改 `db_connector` 字段为 `mysql` 或者 `sqlite`。

### 运行

要运行该应用程序，请使用以下命令：

```bash
python main.py
```
