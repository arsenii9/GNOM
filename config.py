{
    "trading_pairs": [
        {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "indicators": {
                "macd_fast_length": 12,
                "macd_slow_length": 26,
                "macd_signal_length": 9,
                "adx_length": 14,
                "adx_threshold": 25.0,
                "di_threshold": 20.0,
                "atr_length": 14,
                "atr_ma_length": 14,
                "atr_multiplier": 1.0
            },
            "risk_management": {
                "initial_balance": 10000,
                "leverage": 1,
                "stop_loss_percentage": 2.0,
                "take_profit_percentage": 4.0,
                "trailing_stop_activation": True,
                "trailing_stop_percent": 1.0
            }
        },
        {
            "symbol": "ETHUSDT",
            "timeframe": "1h",
            "indicators": {
                "macd_fast_length": 10,
                "macd_slow_length": 21,
                "macd_signal_length": 7,
                "adx_length": 14,
                "adx_threshold": 22.0,
                "di_threshold": 18.0,
                "atr_length": 14,
                "atr_ma_length": 14,
                "atr_multiplier": 1.2
            },
            "risk_management": {
                "initial_balance": 8000,
                "leverage": 1,
                "stop_loss_percentage": 1.5,
                "take_profit_percentage": 3.0,
                "trailing_stop_activation": True,
                "trailing_stop_percent": 0.8
            }
        }

    ],
    "api": {
        "api_key": "aGGCPStZcDzaZKLEIJ4XBDZcUqmY7t2VuUqj1JvkvzvRgnnpS0iMAWDUa93qapvC",
        "api_secret": "q41L36RCIgdnr2Kh3WUrmAElqg9LVqNpN7KBDr1yLyt3AtDfutWwTdrJ6rOfSc2c"
    },
    "connection": {
        "polling_interval": 60
    },
    "telegram": [
        {
            "bot_token": "YOUR_TELEGRAM_BOT_TOKEN_1",
            "chat_id": "YOUR_TELEGRAM_CHAT_ID_1"
        },
        {
            "bot_token": "YOUR_TELEGRAM_BOT_TOKEN_2",
            "chat_id": "YOUR_TELEGRAM_CHAT_ID_2"
        }

    ],
    "log_level": "INFO",
    "daily_loss_limit": 1000,
"max_position_size": 0.1,
"max_open_positions": 5,
"trailing_take_profit_distance": 1.0,
"stop_loss_percentage": 2.0,
"take_profit_percentage": 4.0,
"pnl_threshold_start": 100.0,
"pnl_threshold_stop": 500.0,
"api_semaphore_limit": 10,
"min_trade_interval": 300,
"adx_threshold": 25.0,
"database": "bot.db"
}
