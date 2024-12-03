# main.py

import os
import sys
import asyncio
import logging
import re
from datetime import datetime, timedelta
import signal
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
import aiohttp
import pandas as pd
import ta
from binance import AsyncClient
from binance.exceptions import BinanceAPIException
from binance.helpers import round_step_size
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from collections import OrderedDict
from asyncio_throttle import Throttler
import json

# Установка точности Decimal
getcontext().prec = 28

# Асинхронная загрузка конфигурации из config.json
async def load_config() -> Dict[str, Any]:
    """Загрузка конфигурационного файла."""
    try:
        with open('config.json', 'r', encoding='utf-8') as file:
            config = json.load(file)
        return config
    except Exception as e:
        print(f"Ошибка при загрузке конфигурации: {e}")
        sys.exit(1)

# Настройка логирования
class SensitiveFilter(logging.Filter):
    """Фильтр для маскировки чувствительной информации в логах."""
    def __init__(self, api_key: str, api_secret: str):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret

    def filter(self, record):
        message = record.getMessage()
        if self.api_key:
            message = re.sub(re.escape(self.api_key), '***', message)
        if self.api_secret:
            message = re.sub(re.escape(self.api_secret), '***', message)
        record.msg = message
        return True

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """Настройка логирования."""
    try:
        logger = logging.getLogger('crypto_bot')
        logger.setLevel(getattr(logging, config.get('log_level', 'INFO').upper(), logging.INFO))

        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s')

        file_handler = logging.FileHandler("crypto_bot.log", encoding='utf-8')
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        api_key = config['api'].get('api_key', '')
        api_secret = config['api'].get('api_secret', '')
        logger.addFilter(SensitiveFilter(api_key, api_secret))

        return logger
    except Exception as e:
        print(f"Ошибка при настройке логирования: {e}")
        sys.exit(1)

# Ограничитель скорости для Binance API
class BinanceThrottler:
    """Ограничитель скорости для Binance API."""
    def __init__(self, rate_limit: int = 1200, period: int = 60):
        self.throttler = Throttler(rate_limit=rate_limit, period=period)
        self.endpoint_weights = {
            'futures_klines': 1,
            'futures_account_balance': 5,
            'futures_exchange_info': 1,
            'futures_symbol_ticker': 1,
            'futures_get_order': 1,
            'futures_create_order': 1,
            'futures_cancel_order': 1,
            'futures_change_leverage': 1,
            'futures_change_margin_type': 1,
            # Добавьте другие эндпоинты и их веса при необходимости
        }

    async def rate_limited_api_call(self, endpoint: str, coroutine):
        weight = self.endpoint_weights.get(endpoint, 1)
        for _ in range(weight):
            await self.throttler.acquire()
        return await coroutine

# Кэш для информации о символах
class LRUCache:
    """Простой LRU-кэш для хранения информации о символах."""
    def __init__(self, capacity: int = 100):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    async def set(self, key: str, value: Any):
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

symbol_info_cache = LRUCache(capacity=200)

# Управление уведомлениями
class NotificationManager:
    """Класс для управления отправкой уведомлений."""
    def __init__(self, telegram_configs: List[Dict[str, str]]):
        self.telegram_configs = telegram_configs
        self.session = None  # Инициализируем как None

    async def init_session(self):
        """Инициализация aiohttp сессии."""
        self.session = aiohttp.ClientSession()

    async def send_notification(self, message: str, logger: logging.Logger):
        """Отправка уведомления через все Telegram аккаунты асинхронно."""
        if not self.telegram_configs:
            logger.info("Telegram настройки не заданы. Уведомление не отправлено.")
            return
        tasks = []
        for tg in self.telegram_configs:
            tasks.append(asyncio.create_task(self._send_message(tg['bot_token'], tg['chat_id'], message, logger)))
        await asyncio.gather(*tasks)

    async def _send_message(self, bot_token: str, chat_id: str, message: str, logger: logging.Logger):
        """Асинхронная отправка сообщения одному Telegram аккаунту."""
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message
        }
        try:
            async with self.session.post(url, data=payload, timeout=10) as resp:
                if resp.status != 200:
                    response_text = await resp.text()
                    logger.error(f"Ошибка при отправке уведомления в Telegram (Chat ID: {chat_id}): {resp.status}")
                    logger.error(f"Текст ответа Telegram API: {response_text}")
        except asyncio.TimeoutError:
            logger.error(f"Тайм-аут при отправке уведомления в Telegram (Chat ID: {chat_id}).")
        except Exception as e:
            logger.exception(f"Ошибка при отправке уведомления в Telegram (Chat ID: {chat_id}): {e}")

    async def close(self):
        """Закрытие сессии aiohttp."""
        if self.session:
            await self.session.close()

# Главный класс торгового бота
class TradingBot:
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.shutdown_event = asyncio.Event()
        self.trade_times_lock = asyncio.Lock()
        self.positions_lock = asyncio.Lock()
        self.throttler = BinanceThrottler()
        self.symbol_info_cache = symbol_info_cache

        # Параметры управления рисками
        self.daily_loss_limit = Decimal(str(self.config['daily_loss_limit']))
        self.total_daily_loss = Decimal('0.0')
        self.max_position_size = Decimal(str(self.config['max_position_size']))
        self.max_open_positions = int(self.config['max_open_positions'])
        self.min_trade_interval = float(self.config['min_trade_interval'])
        self.adx_threshold = Decimal(str(self.config['adx_threshold']))

        # Настройка Telegram уведомлений
        self.notification_manager = NotificationManager(self.config.get('telegram', []))

    async def validate_config(self):
        """Валидация конфигурационных параметров."""
        required_keys = [
            'api', 'trading_pairs', 'connection', 'telegram',
            'log_level', 'daily_loss_limit', 'max_position_size',
            'max_open_positions', 'min_trade_interval',
            'adx_threshold', 'stop_loss_percentage', 'take_profit_percentage',
            'database'
        ]
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            self.logger.error(f"Отсутствуют необходимые конфигурационные параметры: {', '.join(missing_keys)}")
            sys.exit(1)

        # Дополнительная проверка пути к базе данных
        database_path = self.config['database']
        if not isinstance(database_path, str) or not database_path.strip():
            self.logger.error("Параметр 'database' должен быть непустой строкой.")
            sys.exit(1)
        self.logger.debug(f"Путь к базе данных: {database_path}")

    async def initialize_database(self, db: aiosqlite.Connection):
        """Инициализация базы данных."""
        try:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    symbol TEXT PRIMARY KEY,
                    quantity REAL,
                    entry_price REAL,
                    status TEXT,
                    direction TEXT,
                    order_ids TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    pnl REAL DEFAULT 0,
                    is_set_aside INTEGER DEFAULT 0,
                    commission REAL DEFAULT 0
                )
            ''')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_status ON positions (status)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON positions (symbol)')
            await db.commit()
        except aiosqlite.Error as e:
            self.logger.exception(f"Ошибка инициализации базы данных: {e}")

    async def save_position(self, db: aiosqlite.Connection, symbol: str, data: Dict[str, Any]) -> bool:
        """Сохранение позиции в базе данных."""
        try:
            await db.execute('''
                INSERT OR REPLACE INTO positions (symbol, quantity, entry_price, status, direction, order_ids, pnl, is_set_aside, commission)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                float(data.get('quantity', 0)),
                float(data.get('entry_price', 0)),
                data.get('status', 'open'),
                data.get('direction', 'LONG'),
                json.dumps(data.get('order_ids', {})),
                float(data.get('pnl', 0)),
                int(data.get('is_set_aside', 0)),
                float(data.get('commission', 0))
            ))
            await db.commit()
            return True
        except aiosqlite.Error as e:
            self.logger.exception(f"Ошибка при сохранении позиции для {symbol}: {e}")
            return False

    async def get_open_positions(self, db: aiosqlite.Connection) -> Dict[str, Dict[str, Any]]:
        """Получение всех открытых позиций."""
        try:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('SELECT * FROM positions WHERE status = "open"')
            rows = await cursor.fetchall()
            positions = {}
            for row in rows:
                positions[row['symbol']] = {
                    'quantity': Decimal(str(row['quantity'])),
                    'entry_price': Decimal(str(row['entry_price'])),
                    'status': row['status'],
                    'direction': row['direction'],
                    'order_ids': json.loads(row['order_ids']),
                    'pnl': Decimal(str(row['pnl'])),
                    'is_set_aside': bool(row['is_set_aside']),
                    'commission': Decimal(str(row['commission'])),
                }
            return positions
        except aiosqlite.Error as e:
            self.logger.exception(f"Ошибка при получении открытых позиций: {e}")
            return {}

    async def remove_position(self, db: aiosqlite.Connection, symbol: str):
        """Удаление позиции из базы данных."""
        try:
            await db.execute('DELETE FROM positions WHERE symbol = ?', (symbol,))
            await db.commit()
        except aiosqlite.Error as e:
            self.logger.exception(f"Ошибка при удалении позиции для {symbol}: {e}")

    async def get_closed_positions(self, db: aiosqlite.Connection) -> List[Dict[str, Any]]:
        """Получение всех закрытых позиций."""
        try:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('SELECT * FROM positions WHERE status = "closed"')
            rows = await cursor.fetchall()
            positions = []
            for row in rows:
                positions.append({
                    'symbol': row['symbol'],
                    'quantity': Decimal(str(row['quantity'])),
                    'entry_price': Decimal(str(row['entry_price'])),
                    'status': row['status'],
                    'direction': row['direction'],
                    'order_ids': json.loads(row['order_ids']),
                    'pnl': Decimal(str(row['pnl'])),
                    'is_set_aside': bool(row['is_set_aside']),
                    'commission': Decimal(str(row['commission'])),
                })
            return positions
        except aiosqlite.Error as e:
            self.logger.exception(f"Ошибка при получении закрытых позиций: {e}")
            return []

    async def remove_closed_position(self, db: aiosqlite.Connection, symbol: str):
        """Удаление закрытой позиции из базы данных."""
        try:
            await db.execute('DELETE FROM positions WHERE symbol = ?', (symbol,))
            await db.commit()
        except aiosqlite.Error as e:
            self.logger.exception(f"Ошибка при удалении закрытой позиции для {symbol}: {e}")

    # Функции для работы с Binance API
    async def fetch_historical_data(self, client: AsyncClient, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
        """Загрузка исторических данных для символа."""
        try:
            klines = await self.throttler.rate_limited_api_call(
                'futures_klines',
                client.futures_klines(symbol=symbol, interval=timeframe, limit=limit)
            )
            if not klines:
                self.logger.warning(f"Не удалось получить исторические данные для {symbol}.")
                return None
            df = pd.DataFrame(klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_columns] = df[numeric_columns].astype(float)
            df.set_index('open_time', inplace=True)

            self.logger.debug(f"Получено {len(df)} строк исторических данных для {symbol}.")

            return df[['open', 'high', 'low', 'close', 'volume']]
        except BinanceAPIException as e:
            self.logger.error(f"Binance API ошибка при получении исторических данных для {symbol}: {e}")
            await self.notification_manager.send_notification(f"Ошибка при получении исторических данных для {symbol}: {e}", self.logger)
            return None
        except asyncio.TimeoutError:
            self.logger.error(f"Тайм-аут при получении исторических данных для {symbol}.")
            await self.notification_manager.send_notification(f"Тайм-аут при получении исторических данных для {symbol}.", self.logger)
            return None
        except Exception as e:
            self.logger.exception(f"Ошибка при получении исторических данных для {symbol}: {e}")
            await self.notification_manager.send_notification(f"Ошибка при получении исторических данных для {symbol}: {e}", self.logger)
            return None

    async def get_account_balance(self, client: AsyncClient, asset: str = 'USDT') -> Tuple[Decimal, Decimal]:
        """Получение баланса аккаунта для определенного актива с повторными попытками."""
        try:
            account_balances = await self.throttler.rate_limited_api_call(
                'futures_account_balance',
                client.futures_account_balance()
            )
            for balance in account_balances:
                if balance['asset'] == asset:
                    total_balance = Decimal(balance['balance'])
                    available_balance = Decimal(balance['availableBalance'])
                    return available_balance, total_balance
            return Decimal('0'), Decimal('0')
        except BinanceAPIException as e:
            self.logger.error(f"Binance API ошибка: {e}")
            return Decimal('0'), Decimal('0')
        except asyncio.TimeoutError:
            self.logger.error("Тайм-аут при получении баланса аккаунта.")
            return Decimal('0'), Decimal('0')
        except Exception as e:
            self.logger.exception("Ошибка при получении баланса аккаунта.")
            return Decimal('0'), Decimal('0')

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10), retry=retry_if_exception_type(BinanceAPIException))
    async def get_symbol_info(self, client: AsyncClient, symbol: str) -> Tuple[Optional[float], Optional[float], Optional[Decimal], Optional[Decimal], Optional[str], Optional[str], Optional[int], Optional[Dict[str, Decimal]]]:
        """Получение информации о символе с кешированием и повторными попытками."""
        try:
            cached_info = await self.symbol_info_cache.get(symbol)
            if cached_info:
                return cached_info
            # Не в кэше, делаем API-вызов с ограничением скорости
            exchange_info = await self.throttler.rate_limited_api_call(
                'futures_exchange_info',
                client.futures_exchange_info()
            )
            symbol_info_list = exchange_info['symbols']
            info = next((item for item in symbol_info_list if item['symbol'] == symbol), None)
            if info:
                filters = {f['filterType']: f for f in info['filters']}
                tick_size = float(filters['PRICE_FILTER']['tickSize']) if 'PRICE_FILTER' in filters else None
                step_size = float(filters['LOT_SIZE']['stepSize']) if 'LOT_SIZE' in filters else None
                min_qty = Decimal(filters['LOT_SIZE']['minQty']) if 'LOT_SIZE' in filters else None

                # Добавляем точность количества (максимальное число знаков после запятой)
                quantity_precision = info.get('quantityPrecision', None)

                # Получаем фильтр PERCENT_PRICE
                percent_price_filter = filters.get('PERCENT_PRICE', {})
                if percent_price_filter:
                    price_limit = {
                        'multiplierDown': Decimal(percent_price_filter['multiplierDown']),
                        'multiplierUp': Decimal(percent_price_filter['multiplierUp']),
                        'multiplierDecimal': int(percent_price_filter.get('multiplierDecimal', 0))
                    }
                else:
                    price_limit = None

                base_asset = info.get('baseAsset')
                quote_asset = info.get('quoteAsset')

                symbol_info = (tick_size, step_size, min_qty, Decimal('0'), base_asset, quote_asset, quantity_precision, price_limit)
                if None not in symbol_info:
                    await self.symbol_info_cache.set(symbol, symbol_info)
                    return symbol_info
                else:
                    self.logger.error(f"Некорректная информация о символе {symbol}: {symbol_info}")
                    return None, None, None, None, None, None, None, None
            else:
                return None, None, None, None, None, None, None, None
        except BinanceAPIException as e:
            self.logger.error(f"Binance API ошибка: {e}")
            return None, None, None, None, None, None, None, None
        except asyncio.TimeoutError:
            self.logger.error(f"Тайм-аут при получении информации о символе {symbol}.")
            return None, None, None, None, None, None, None, None
        except Exception as e:
            self.logger.exception(f"Ошибка при получении информации о символе {symbol}: {e}")
            return None, None, None, None, None, None, None, None

    async def get_current_price(self, client: AsyncClient, symbol: str) -> Optional[Decimal]:
        """Получение текущей цены символа."""
        try:
            ticker = await self.throttler.rate_limited_api_call(
                'futures_symbol_ticker',
                client.futures_symbol_ticker(symbol=symbol)
            )
            return Decimal(ticker['price'])
        except BinanceAPIException as e:
            self.logger.error(f"Binance API ошибка при получении текущей цены для {symbol}: {e}")
            return None
        except asyncio.TimeoutError:
            self.logger.error(f"Тайм-аут при получении текущей цены для {symbol}.")
            return None
        except Exception as e:
            self.logger.exception(f"Ошибка при получении текущей цены для {symbol}: {e}")
            return None

    async def cancel_order(self, client: AsyncClient, symbol: str, order_id: int):
        """Отмена ордера."""
        try:
            await self.throttler.rate_limited_api_call(
                'futures_cancel_order',
                client.futures_cancel_order(symbol=symbol, orderId=order_id)
            )
            self.logger.info(f"Ордер {order_id} для {symbol} отменен.")
        except BinanceAPIException as e:
            self.logger.error(f"Binance API ошибка при отмене ордера {order_id} для {symbol}: {e}")
        except asyncio.TimeoutError:
            self.logger.error(f"Тайм-аут при отмене ордера {order_id} для {symbol}.")
        except Exception as e:
            self.logger.exception(f"Ошибка при отмене ордера {order_id} для {symbol}: {e}")

    async def wait_for_order_fill(self, client: AsyncClient, symbol: str, order_id: int, timeout: int = 60) -> Optional[Dict[str, Any]]:
        """Ожидание исполнения ордера в течение заданного времени."""
        end_time = datetime.utcnow() + timedelta(seconds=timeout)
        while datetime.utcnow() < end_time:
            try:
                order = await self.throttler.rate_limited_api_call(
                    'futures_get_order',
                    client.futures_get_order(symbol=symbol, orderId=order_id)
                )
                if order['status'] == 'FILLED':
                    return order
                elif order['status'] == 'CANCELED':
                    self.logger.warning(f"Ордер {order_id} для {symbol} был отменен.")
                    return None
                await asyncio.sleep(2)  # Ожидаем 2 секунды перед следующей проверкой
            except BinanceAPIException as e:
                self.logger.error(f"Ошибка при получении статуса ордера {order_id} для {symbol}: {e}")
                return None
            except asyncio.TimeoutError:
                self.logger.error(f"Тайм-аут при ожидании ордера {order_id} для {symbol}.")
                return None
            except Exception as e:
                self.logger.exception(f"Ошибка при ожидании ордера {order_id} для {symbol}: {e}")
                return None
        self.logger.warning(f"Ожидание исполнения ордера {order_id} для {symbol} истекло.")
        return None

    # Вычисление технических индикаторов
    def calculate_indicators(self, df: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
        """Вычисление технических индикаторов для символа."""
        try:
            df = df.copy()
            pair_config = next((pair for pair in self.config['trading_pairs'] if pair['symbol'] == symbol), None)
            if not pair_config:
                self.logger.warning(f"Конфигурация для {symbol} не найдена.")
                return None
            indicators = pair_config.get('indicators', {})

            # MACD
            macd = ta.trend.MACD(
                close=df['close'],
                window_slow=int(indicators.get('macd_slow_length', 26)),
                window_fast=int(indicators.get('macd_fast_length', 12)),
                window_sign=int(indicators.get('macd_signal_length', 9))
            )
            df['macd'] = macd.macd()
            df['macd_signal'] = macd.macd_signal()
            df['macd_diff'] = macd.macd_diff()  # Гистограмма MACD

            # ADX и DI+/-
            adx = ta.trend.ADXIndicator(
                high=df['high'],
                low=df['low'],
                close=df['close'],
                window=int(indicators.get('adx_length', 14))
            )
            df['adx'] = adx.adx()
            df['plus_di'] = adx.adx_pos()
            df['minus_di'] = adx.adx_neg()

            # ATR
            atr = ta.volatility.AverageTrueRange(
                high=df['high'],
                low=df['low'],
                close=df['close'],
                window=int(indicators.get('atr_length', 14))
            )
            df['atr'] = atr.average_true_range()
            df['atr_ma'] = df['atr'].rolling(window=int(indicators.get('atr_ma_length', 14))).mean()

            # Преобразование atr_multiplier в float
            atr_multiplier = float(indicators.get('atr_multiplier', 1.0))
            self.logger.debug(f"atr_multiplier: {atr_multiplier} (type: {type(atr_multiplier)})")
            df['atr_threshold'] = df['atr_ma'] * atr_multiplier

            # Проверка на достаточное количество данных
            required_rows = max(
                int(indicators.get('macd_slow_length', 26)) +
                int(indicators.get('atr_ma_length', 14)),
                100  # Минимальное количество строк для стабильных индикаторов
            )
            if len(df) < required_rows:
                self.logger.warning(f"Недостаточно данных для расчёта индикаторов для {symbol}. Требуется минимум {required_rows} строк, получено {len(df)}.")
                return None

            df.dropna(inplace=True)
            if df.empty:
                self.logger.warning(f"После удаления NaN индикаторы не рассчитаны для {symbol}.")
                return None

            return df
        except KeyError as e:
            self.logger.error(f"Отсутствует необходимый столбец при вычислении индикаторов для {symbol}: {e}")
            return None
        except ValueError as e:
            self.logger.error(f"Некорректное значение индикатора при вычислении для {symbol}: {e}")
            return None
        except Exception as e:
            self.logger.exception(f"Неожиданная ошибка при вычислении индикаторов для {symbol}: {e}")
            return None

    # Генерация сигналов на основе индикаторов
    def generate_signals(self, df: pd.DataFrame, symbol: str) -> Tuple[bool, bool]:
        """
        Генерация торговых сигналов на основе пересечений MACD, значений ADX, DI+/- и ATR.
        Возвращает кортеж (buy_signal, sell_signal)
        """
        try:
            latest = df.iloc[-1]
            previous = df.iloc[-2]

            # Пересечение MACD гистограммы
            macd_cross_up = (previous['macd_diff'] < 0) and (latest['macd_diff'] > 0)
            macd_cross_down = (previous['macd_diff'] > 0) and (latest['macd_diff'] < 0)

            # Условия для покупки
            buy_signal = (
                    macd_cross_up and
                    (latest['adx'] >= self.adx_threshold) and
                    (latest['plus_di'] > latest['minus_di']) and
                    (Decimal(latest['plus_di']) >= Decimal(str(self.config['di_threshold']))) and
                    (Decimal(latest['atr']) >= Decimal(str(self.config['atr_multiplier'])))
            )

            # Условия для продажи
            sell_signal = (
                    macd_cross_down and
                    (latest['adx'] >= self.adx_threshold) and
                    (latest['minus_di'] > latest['plus_di']) and
                    (Decimal(latest['minus_di']) >= Decimal(str(self.config['di_threshold']))) and
                    (Decimal(latest['atr']) >= Decimal(str(self.config['atr_multiplier'])))
            )

            return buy_signal, sell_signal
        except Exception as e:
            self.logger.exception(f"Ошибка при генерации сигналов для {symbol}: {e}")
            return False, False

    # Открытие позиции с использованием стоп-лосс и тейк-профит ордеров
    async def open_position_with_sl_tp(self, client: AsyncClient, symbol: str, quantity: Decimal, side: str, leverage: int, notification_manager: NotificationManager) -> Tuple[bool, Optional[Dict[str, Any]], Optional[Decimal], Optional[Decimal]]:
        """Открытие позиции с установкой стоп-лосса и тейк-профита."""
        try:
            # Получение информации о символе
            tick_size, step_size, min_qty, min_notional, base_asset, quote_asset, quantity_precision, price_limit = await self.get_symbol_info(client, symbol)
            if None in [tick_size, step_size, min_qty, min_notional, base_asset, quote_asset, quantity_precision, price_limit]:
                self.logger.error(f"Не удалось получить полную информацию о символе {symbol}.")
                await notification_manager.send_notification(f"Не удалось получить полную информацию о символе {symbol}.", self.logger)
                return False, None, None, None

            # Установка кредитного плеча
            await self.throttler.rate_limited_api_call('futures_change_leverage', client.futures_change_leverage(
                symbol=symbol,
                leverage=leverage
            ))

            # Округление количества до шага
            try:
                step_size_float = float(step_size)
                quantity_float = float(quantity)
                rounded_quantity = round_step_size(quantity_float, step_size_float)
                quantity = Decimal(rounded_quantity)
                self.logger.debug(f"Округленное количество для {symbol}: {quantity}")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Ошибка при округлении количества для {symbol}: {e}")
                await notification_manager.send_notification(f"Ошибка при округлении количества для {symbol}: {e}", self.logger)
                return False, None, None, None

            if quantity < min_qty:
                self.logger.warning(f"Количество для {symbol} ({quantity}) ниже минимального ({min_qty}). Пропуск.")
                return False, None, None, None

            # Получение текущей цены
            current_price = await self.get_current_price(client, symbol)
            if current_price is None:
                self.logger.warning(f"Не удалось получить текущую цену для {symbol}.")
                return False, None, None, None

            # Проверка минимальной суммы ордера
            order_notional = quantity * current_price
            if order_notional < min_notional:
                self.logger.warning(f"Сумма ордера для {symbol} ({order_notional}) меньше минимальной ({min_notional}). Пропуск.")
                return False, None, None, None

            # Размещение рыночного ордера для открытия позиции
            order = await self.throttler.rate_limited_api_call('futures_create_order', client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=format(quantity, f'.{quantity_precision}f')
            ))
            self.logger.info(f"Размещен рыночный {side} ордер для {symbol}: {order}")

            # Получение цены входа
            executed_price = Decimal(order.get('avgPrice', '0'))
            if executed_price == 0:
                # Если avgPrice отсутствует, используем среднюю цену исполнения
                executed_qty = Decimal(order.get('executedQty', '1'))
                cum_quote = Decimal(order.get('cumQuote', '0'))
                if executed_qty > 0:
                    executed_price = cum_quote / executed_qty
                else:
                    executed_price = Decimal('0')

            if executed_price == 0:
                self.logger.error(f"Не удалось определить цену исполнения для {symbol}.")
                await notification_manager.send_notification(f"Не удалось определить цену исполнения для {symbol}.", self.logger)
                return False, None, None, None

            # Установка стоп-лосса и тейк-профита
            stop_loss_percentage = Decimal(str(self.config['stop_loss_percentage'])) / Decimal('100')
            take_profit_percentage = Decimal(str(self.config['take_profit_percentage'])) / Decimal('100')

            if side == 'BUY':
                stop_price = executed_price * (Decimal('1') - stop_loss_percentage)
                take_profit_price = executed_price * (Decimal('1') + take_profit_percentage)
                sl_side = 'SELL'
                tp_side = 'SELL'
            else:
                stop_price = executed_price * (Decimal('1') + stop_loss_percentage)
                take_profit_price = executed_price * (Decimal('1') - take_profit_percentage)
                sl_side = 'BUY'
                tp_side = 'BUY'

            # Округление цен до правильного шага
            stop_price = float(round_step_size(float(stop_price), float(tick_size)))
            take_profit_price = float(round_step_size(float(take_profit_price), float(tick_size)))

            # Размещение стоп-лосс ордера
            stop_order = await self.throttler.rate_limited_api_call('futures_create_order', client.futures_create_order(
                symbol=symbol,
                side=sl_side,
                type='STOP_MARKET',
                stopPrice=stop_price,
                closePosition=True,
                timeInForce='GTC'  # Good Till Cancelled
            ))
            self.logger.info(f"Размещен стоп-лосс ордер для {symbol}: {stop_order}")

            # Размещение тейк-профит ордера
            tp_order = await self.throttler.rate_limited_api_call('futures_create_order', client.futures_create_order(
                symbol=symbol,
                side=tp_side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=take_profit_price,
                closePosition=True,
                timeInForce='GTC'
            ))
            self.logger.info(f"Размещен тейк-профит ордер для {symbol}: {tp_order}")

            order_ids = {
                'order_id': order.get('orderId'),
                'stop_order_id': stop_order.get('orderId'),
                'tp_order_id': tp_order.get('orderId')
            }

            return True, order_ids, executed_price, Decimal('0')
        except BinanceAPIException as e:
            self.logger.error(f"Binance API ошибка при открытии позиции для {symbol}: {e}")
            await notification_manager.send_notification(f"Ошибка при открытии позиции для {symbol}: {e}", self.logger)
            return False, None, None, None
        except Exception as e:
            self.logger.exception(f"Неожиданная ошибка при открытии позиции для {symbol}: {e}")
            await notification_manager.send_notification(f"Ошибка при открытии позиции для {symbol}: {e}", self.logger)
            return False, None, None, None

    async def open_new_position(self, client: AsyncClient, db: aiosqlite.Connection, symbol: str, direction: str, notification_manager: NotificationManager):
        """Открытие новой позиции в указанном направлении с использованием стоп-лосс и тейк-профит ордеров."""
        try:
            pair_config = next((pair for pair in self.config['trading_pairs'] if pair['symbol'] == symbol), None)
            if not pair_config:
                self.logger.error(f"Конфигурация для {symbol} не найдена.")
                await notification_manager.send_notification(f"Конфигурация для {symbol} не найдена.", self.logger)
                return

            # Получение баланса
            free_balance, total_balance = await self.get_account_balance(client, asset='USDT')
            if total_balance == 0:
                self.logger.warning(f"Нет доступного баланса для USDT. Пропуск {symbol}.")
                return

            # Определение размера позиции с учётом максимального размера позиции
            current_price = await self.get_current_price(client, symbol)
            if current_price is None:
                self.logger.warning(f"Не удалось получить текущую цену для {symbol}.")
                return

            position_value = free_balance * self.max_position_size
            quantity = position_value / current_price

            # Получение информации о символе
            tick_size, step_size, min_qty, min_notional, base_asset, quote_asset, quantity_precision, price_limit = await self.get_symbol_info(client, symbol)
            if None in [tick_size, step_size, min_qty, min_notional, base_asset, quote_asset, quantity_precision, price_limit]:
                self.logger.error(f"Не удалось получить полную информацию о символе {symbol}. Пропуск торговли.")
                await notification_manager.send_notification(f"Не удалось получить полную информацию о символе {symbol}.", self.logger)
                return

            # Округление количества до шага
            try:
                step_size_float = float(step_size)
                quantity_float = float(quantity)
                rounded_quantity = round_step_size(quantity_float, step_size_float)
                quantity = Decimal(rounded_quantity)
                self.logger.debug(f"Округленное количество для {symbol}: {quantity}")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Ошибка при округлении количества для {symbol}: {e}")
                await notification_manager.send_notification(f"Ошибка при округлении количества для {symbol}: {e}", self.logger)
                return

            quantity_str = format(quantity, f'.{quantity_precision}f')

            if quantity < min_qty:
                self.logger.warning(f"Количество для {symbol} ({quantity}) ниже минимального ({min_qty}). Пропуск.")
                return

            # Проверка достаточного баланса
            order_notional = quantity * current_price
            if order_notional < min_notional:
                self.logger.warning(f"Сумма ордера для {symbol} ({order_notional}) меньше минимальной ({min_notional}). Пропуск.")
                return

            side = 'BUY' if direction == 'LONG' else 'SELL'
            leverage = int(pair_config['risk_management'].get('leverage', 1))

            success, order_ids, executed_price, _ = await self.open_position_with_sl_tp(client, symbol, quantity, side, leverage, notification_manager)
            if success:
                position_data = {
                    'quantity': quantity,
                    'entry_price': executed_price,
                    'status': 'open',
                    'direction': direction,
                    'order_ids': order_ids,
                    'is_set_aside': False,
                }
                save_success = await self.save_position(db, symbol, position_data)
                if not save_success:
                    self.logger.error(f"Не удалось сохранить позицию для {symbol}.")
                    await notification_manager.send_notification(f"Не удалось сохранить позицию для {symbol}.", self.logger)
                    return
            else:
                self.logger.error(f"Не удалось открыть позицию для {symbol}.")
        except Exception as e:
            self.logger.exception(f"Ошибка при открытии новой позиции для {symbol}: {e}")
            await self.notification_manager.send_notification(f"Ошибка при открытии новой позиции для {symbol}: {e}", self.logger)

    async def manage_positions(self, client: AsyncClient, db: aiosqlite.Connection, notification_manager: NotificationManager):
        """Обновление состояния открытых позиций."""
        try:
            open_positions = await self.get_open_positions(db)

            for symbol, position in open_positions.items():
                # Получение текущей цены
                current_price = await self.get_current_price(client, symbol)
                if current_price is None:
                    continue

                entry_price = position['entry_price']
                quantity = position['quantity']
                direction = position['direction']
                pnl = position['pnl']

                # Вычисление текущего PnL
                if direction == 'LONG':
                    current_pnl = (current_price - entry_price) * quantity
                else:
                    current_pnl = (entry_price - current_price) * quantity

                async with self.trade_times_lock:
                    if current_pnl < 0:
                        if self.total_daily_loss + abs(current_pnl) > self.daily_loss_limit:
                            self.logger.warning(f"Достигнут дневной лимит убытков ({self.daily_loss_limit} USDT). Закрытие позиции {symbol}.")
                            await self.close_position(client, db, symbol, notification_manager)
                            continue
                        else:
                            self.total_daily_loss += abs(current_pnl)

                # Логирование и обновление PnL
                self.logger.info(f"[{symbol}] Текущий PnL: {current_pnl}")

                # Проверка на превышение PnL порогов для трейлинг-стопа
                if pnl < Decimal(str(self.config['pnl_threshold_start'])) and pnl + Decimal(str(current_pnl)) >= Decimal(str(self.config['pnl_threshold_start'])):
                    # Начало трейлинг-стопа
                    self.logger.info(f"[{symbol}] Достигнут PnL порог ({self.config['pnl_threshold_start']} USDT). Активируем трейлинг-стоп.")
                    await self.update_trailing_stop(client, db, symbol, current_price, notification_manager)
                elif pnl >= Decimal(str(self.config['pnl_threshold_stop'])):
                    # Остановка трейлинг-стопа
                    self.logger.info(f"[{symbol}] Превышен PnL порог ({self.config['pnl_threshold_stop']} USDT). Останавливаем трейлинг-стоп.")
                    await self.stop_trailing_stop(client, db, symbol, notification_manager)

        except Exception as e:
            self.logger.exception(f"Ошибка при управлении позициями: {e}")

    async def update_trailing_stop(self, client: AsyncClient, db: aiosqlite.Connection, symbol: str, current_price: Decimal, notification_manager: NotificationManager):
        """Обновление трейлинг-стопа для позиции."""
        try:
            pair_config = next((pair for pair in self.config['trading_pairs'] if pair['symbol'] == symbol), None)
            if not pair_config:
                self.logger.error(f"Конфигурация для {symbol} не найдена.")
                await notification_manager.send_notification(f"Конфигурация для {symbol} не найдена.", self.logger)
                return

            indicators = pair_config.get('indicators', {})
            trailing_stop_percent = Decimal(str(indicators.get('atr_multiplier', 1.0)))

            # Вычисление новой цены для трейлинг-стопа
            position = await self.get_open_positions(db)
            if symbol not in position:
                self.logger.warning(f"Позиция для {symbol} не найдена при обновлении трейлинг-стопа.")
                return

            direction = position[symbol]['direction']
            if direction == 'LONG':
                new_trailing_stop = current_price * (Decimal('1') - trailing_stop_percent / Decimal('100'))
                # Здесь предполагается, что стоп-лосс ордер уже установлен при открытии позиции
                # Если необходимо, можно добавить логику для обновления стоп-лосс ордера
                self.logger.info(f"[{symbol}] Обновлен трейлинг-стоп на {new_trailing_stop}")
                await notification_manager.send_notification(f"[{symbol}] Обновлен трейлинг-стоп на {new_trailing_stop}", self.logger)
            elif direction == 'SHORT':
                new_trailing_stop = current_price * (Decimal('1') + trailing_stop_percent / Decimal('100'))
                self.logger.info(f"[{symbol}] Обновлен трейлинг-стоп на {new_trailing_stop}")
                await notification_manager.send_notification(f"[{symbol}] Обновлен трейлинг-стоп на {new_trailing_stop}", self.logger)
        except Exception as e:
            self.logger.exception(f"Ошибка при обновлении трейлинг-стопа для {symbol}: {e}")

    async def stop_trailing_stop(self, client: AsyncClient, db: aiosqlite.Connection, symbol: str, notification_manager: NotificationManager):
        """Остановка трейлинг-стопа для позиции."""
        try:
            # Здесь можно добавить логику для удаления или деактивации трейлинг-стопа
            # Например, отмена ордеров стоп-лосс и тейк-профит, если они были установлены
            self.logger.info(f"[{symbol}] Трейлинг-стоп остановлен.")
            await notification_manager.send_notification(f"[{symbol}] Трейлинг-стоп остановлен.", self.logger)
        except Exception as e:
            self.logger.exception(f"Ошибка при остановке трейлинг-стопа для {symbol}: {e}")

    async def trading_logic(self, client: AsyncClient, db: aiosqlite.Connection, symbol: str, notification_manager: NotificationManager):
        """Торговая логика для одного символа на основе индикаторов."""
        try:
            pair_config = next((pair for pair in self.config['trading_pairs'] if pair['symbol'] == symbol), None)
            if not pair_config:
                self.logger.warning(f"Конфигурация для {symbol} не найдена.")
                return

            timeframe = pair_config['timeframe']
            df = await self.fetch_historical_data(client, symbol, timeframe, limit=500)
            if df is None or df.empty:
                return
            df = self.calculate_indicators(df, symbol)
            if df is None or df.empty:
                self.logger.warning(f"Недостаточно данных после вычисления индикаторов для {symbol}.")
                return

            buy_signal, sell_signal = self.generate_signals(df, symbol)

            # Проверка сигналов и открытие позиций
            if buy_signal:
                async with self.positions_lock:
                    open_positions = await self.get_open_positions(db)
                    if symbol not in open_positions and len(open_positions) < self.max_open_positions:
                        self.logger.info(f"Сигнал на покупку для {symbol}.")
                        await self.open_new_position(client, db, symbol, 'LONG', notification_manager)
            elif sell_signal:
                async with self.positions_lock:
                    open_positions = await self.get_open_positions(db)
                    if symbol not in open_positions and len(open_positions) < self.max_open_positions:
                        self.logger.info(f"Сигнал на продажу для {symbol}.")
                        await self.open_new_position(client, db, symbol, 'SHORT', notification_manager)
        except Exception as e:
            self.logger.exception(f"Ошибка в торговой логике для {symbol}: {e}")
            await self.notification_manager.send_notification(f"Ошибка в торговой логике для {symbol}: {e}", self.logger)

    async def close_position(self, client: AsyncClient, db: aiosqlite.Connection, symbol: str, notification_manager: NotificationManager):
        """Закрытие позиции."""
        try:
            open_positions = await self.get_open_positions(db)
            if symbol not in open_positions:
                self.logger.warning(f"Попытка закрыть несуществующую позицию для {symbol}.")
                return

            position = open_positions[symbol]
            direction = position['direction']
            order_ids = position['order_ids']

            # Закрытие позиции путем размещения противоположного рыночного ордера
            side = 'SELL' if direction == 'LONG' else 'BUY'
            quantity = position['quantity']

            # Получение информации о символе
            tick_size, step_size, min_qty, min_notional, base_asset, quote_asset, quantity_precision, price_limit = await self.get_symbol_info(client, symbol)
            if None in [tick_size, step_size, min_qty, min_notional, base_asset, quote_asset, quantity_precision, price_limit]:
                self.logger.error(f"Не удалось получить полную информацию о символе {symbol}. Пропуск закрытия позиции.")
                await notification_manager.send_notification(f"Не удалось получить полную информацию о символе {symbol}.", self.logger)
                return

            # Округление количества до шага
            try:
                step_size_float = float(step_size)
                quantity_float = float(quantity)
                rounded_quantity = round_step_size(quantity_float, step_size_float)
                quantity = Decimal(rounded_quantity)
                self.logger.debug(f"Округленное количество для закрытия {symbol}: {quantity}")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Ошибка при округлении количества для закрытия {symbol}: {e}")
                await notification_manager.send_notification(f"Ошибка при округлении количества для закрытия {symbol}: {e}", self.logger)
                return

            quantity_str = format(quantity, f'.{quantity_precision}f')

            # Размещение рыночного ордера для закрытия позиции
            order = await self.throttler.rate_limited_api_call('futures_create_order', client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=quantity_str
            ))
            self.logger.info(f"Размещен рыночный {side} ордер для закрытия позиции {symbol}: {order}")
            await notification_manager.send_notification(f"Размещен рыночный {side} ордер для закрытия позиции {symbol}: {order}", self.logger)

            # Обновление позиции в базе данных
            position_data = {
                'status': 'closed',
                'pnl': float(order.get('cumQuote', '0'))  # Пример расчета PnL
            }
            save_success = await self.save_position(db, symbol, position_data)
            if save_success:
                self.logger.info(f"Позиция по {symbol} закрыта с PnL: {position_data['pnl']}")
                await notification_manager.send_notification(f"Позиция по {symbol} закрыта с PnL: {position_data['pnl']}", self.logger)
            else:
                self.logger.error(f"Не удалось обновить статус позиции для {symbol}.")
                await notification_manager.send_notification(f"Не удалось обновить статус позиции для {symbol}.", self.logger)
        except Exception as e:
            self.logger.exception(f"Ошибка при закрытии позиции для {symbol}: {e}")
            await notification_manager.send_notification(f"Ошибка при закрытии позиции для {symbol}: {e}", self.logger)

    # Функция сброса дневного лимита убытков
    async def reset_daily_loss(self):
        """Сброс дневного лимита убытков в начале каждого дня UTC."""
        while not self.shutdown_event.is_set():
            now = datetime.utcnow()
            next_reset = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            sleep_duration = (next_reset - now).total_seconds()
            try:
                await asyncio.sleep(sleep_duration)
                async with self.trade_times_lock:
                    self.total_daily_loss = Decimal('0.0')
                self.logger.info("Дневной лимит убытков был сброшен.")
                await self.notification_manager.send_notification("Дневной лимит убытков был сброшен.", self.logger)
            except asyncio.CancelledError:
                self.logger.info("Задача сброса дневного лимита убытков отменена.")
                break
            except Exception as e:
                self.logger.exception(f"Ошибка при сбросе дневного лимита убытков: {e}")

    def handle_exit(self):
        """Обработка сигналов завершения."""
        self.shutdown_event.set()
        self.logger.info("Получен сигнал завершения. Выход из программы...")

    # Основная функция
    async def main(self):
        await self.validate_config()
        self.logger.debug(f"Leverage: {self.config.get('leverage', 1)}")
        self.logger.debug(f"ADX Threshold: {self.adx_threshold}")

        # Инициализация Binance клиента
        self.client = await AsyncClient.create(
            self.config['api']['api_key'],
            self.config['api']['api_secret']
        )

        # Установка URL для фьючерсной тестовой сети, если используется тестнет
        if self.config.get('testnet', False):
            self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'

        # Инициализация базы данных
        db = await aiosqlite.connect(self.config['database'])
        await self.initialize_database(db)

        # Инициализация NotificationManager
        await self.notification_manager.init_session()

        # Обработка сигналов завершения
        if sys.platform != 'win32':
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self.handle_exit)
                except NotImplementedError:
                    self.logger.warning("Обработка сигналов не поддерживается на этой платформе.")
        else:
            self.logger.info("Обработка сигналов недоступна на Windows. Используйте Ctrl+C для выхода.")

        # Запуск задачи сброса дневного лимита убытков
        asyncio.create_task(self.reset_daily_loss())

        # Основной цикл
        try:
            while not self.shutdown_event.is_set():
                tasks = []
                for pair in self.config['trading_pairs']:
                    symbol = pair['symbol']
                    tasks.append(self.trading_logic(self.client, db, symbol, self.notification_manager))
                await asyncio.gather(*tasks)
                await self.manage_positions(self.client, db, self.notification_manager)
                await asyncio.sleep(float(self.config['min_trade_interval']))
        except Exception as e:
            self.logger.exception(f"Фатальная ошибка в основном цикле: {e}")
        finally:
            await db.close()
            await self.client.close_connection()
            await self.notification_manager.close()
            self.logger.info("Бот был остановлен.")

# Запуск бота
async def run_bot():
    config = await load_config()
    logger = setup_logging(config)
    bot = TradingBot(config, logger)
    await bot.main()

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("Бот прерван пользователем.")
