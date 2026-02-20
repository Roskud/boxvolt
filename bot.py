import asyncio
import contextlib
import datetime as dt
import html
import hashlib
import hmac
import json
import io
import os
import re
import sqlite3
import tarfile
import uuid
import base64
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit

import httpx
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InputMediaDocument,
    InputMediaPhoto,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    WebAppInfo,
)
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value if value is not None else ""


def parse_int_list_env(
    raw: str,
    *,
    min_value: int = 1,
    max_value: int = 100000,
    default: list[int] | None = None,
    sort_desc: bool = False,
) -> list[int]:
    values: set[int] = set()
    for chunk in str(raw or "").replace(";", ",").split(","):
        part = chunk.strip()
        if not part:
            continue
        if not re.fullmatch(r"-?\d+", part):
            continue
        number = int(part)
        if number < min_value or number > max_value:
            continue
        values.add(number)
    if not values:
        values = set(default or [])
    ordered = sorted(values, reverse=sort_desc)
    return ordered if ordered else list(default or [])


def parse_csv_env(raw: str, *, default: list[str] | None = None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for chunk in str(raw or "").replace(";", ",").split(","):
        part = chunk.strip()
        if not part:
            continue
        if part in seen:
            continue
        seen.add(part)
        result.append(part)
    if result:
        return result
    return list(default or [])


@dataclass(frozen=True)
class Plan:
    code: str
    title: str
    days: int
    amount_rub: int
    base_amount_rub: int = 0
    discount_percent: int = 0


BOT_TOKEN = env("BOT_TOKEN", required=True)
DB_PATH = env("DB_PATH", str(BASE_DIR / "users.db"))

# 3x-ui / VLESS
XUI_URL = env("XUI_URL", "http://185.23.19.82:2053").rstrip("/")
XUI_USERNAME = env("XUI_USERNAME", "")
XUI_PASSWORD = env("XUI_PASSWORD", "")
INBOUND_ID = int(env("INBOUND_ID", "1"))
SERVER_IP = env("SERVER_IP", "185.23.19.82")
SERVER_PORT = int(env("SERVER_PORT", "443"))
SERVER_COUNTRY = env("SERVER_COUNTRY", "🌍 Global")
SERVER_FLAG_EMOJI = env("SERVER_FLAG_EMOJI", "🌍")
SERVER_NODE_PREFIX = env("SERVER_NODE_PREFIX", "AUTO")
PUBLIC_KEY = env("PUBLIC_KEY", "")
SHORT_ID = env("SHORT_ID", "")
SNI = env("SNI", "www.cloudflare.com")
UTLS_FP = env("UTLS_FP", "chrome")
XUI_FLOW = env("XUI_FLOW", "")
XUI_LIMIT_IP = int(env("XUI_LIMIT_IP", "0"))
XUI_TOTAL_GB = int(env("XUI_TOTAL_GB", "0"))
SUBSCRIPTION_DISPLAY_TOTAL_GB = int(env("SUBSCRIPTION_DISPLAY_TOTAL_GB", "0"))
SPEED_PROFILE_ENABLED = env("SPEED_PROFILE_ENABLED", "1") == "1"
SPEED_PROFILE_NAME = env("SPEED_PROFILE_NAME", "Скоростной")
SPEED_PROFILE_FLOW = env("SPEED_PROFILE_FLOW", "").strip()
SPEED_INBOUND_ID = int(env("SPEED_INBOUND_ID", "0"))
YOUTUBE_PROFILE_ENABLED = env("YOUTUBE_PROFILE_ENABLED", "0") == "1"
YOUTUBE_PROFILE_NAME = env("YOUTUBE_PROFILE_NAME", "Ютуб без рекламы")
YOUTUBE_PROFILE_FLOW = env("YOUTUBE_PROFILE_FLOW", "").strip()
YOUTUBE_INBOUND_ID = int(env("YOUTUBE_INBOUND_ID", "0"))
REALITY_PROFILE_CACHE_SECONDS = int(env("REALITY_PROFILE_CACHE_SECONDS", "300"))

# DonatePay
DONATEPAY_API_KEY = env("DONATEPAY_API_KEY", "").strip()
DONATEPAY_API_BASE = env("DONATEPAY_API_BASE", "https://donatepay.ru/api/v1").strip().rstrip("/")
DONATEPAY_DONATE_BASE_URL = env("DONATEPAY_DONATE_BASE_URL", "").strip()
DONATEPAY_WEBHOOK_SECRET = env("DONATEPAY_WEBHOOK_SECRET", "").strip()
DONATEPAY_WEBHOOK_PATH = env("DONATEPAY_WEBHOOK_PATH", "/donatepay/webhook").strip()
DONATEPAY_POLL_ENABLED = env("DONATEPAY_POLL_ENABLED", "1") == "1"
DONATEPAY_POLL_INTERVAL_SECONDS = int(env("DONATEPAY_POLL_INTERVAL_SECONDS", "20"))
DONATEPAY_POLL_LIMIT = max(1, int(env("DONATEPAY_POLL_LIMIT", "50")))

# CryptoBot (Crypto Pay API)
CRYPTOBOT_ENABLED = env("CRYPTOBOT_ENABLED", "0") == "1"
CRYPTOBOT_API_TOKEN = env("CRYPTOBOT_API_TOKEN", "").strip()
CRYPTOBOT_API_BASE = env("CRYPTOBOT_API_BASE", "https://pay.crypt.bot/api").strip().rstrip("/")
CRYPTOBOT_WEBHOOK_PATH = env("CRYPTOBOT_WEBHOOK_PATH", "/cryptobot/webhook").strip()
CRYPTOBOT_WEBHOOK_SECRET = env("CRYPTOBOT_WEBHOOK_SECRET", "").strip()
CRYPTOBOT_POLL_ENABLED = env("CRYPTOBOT_POLL_ENABLED", "1") == "1"
CRYPTOBOT_POLL_INTERVAL_SECONDS = max(10, int(env("CRYPTOBOT_POLL_INTERVAL_SECONDS", "30")))
CRYPTOBOT_FIAT = (env("CRYPTOBOT_FIAT", "RUB").strip().upper() or "RUB")
CRYPTOBOT_VALIDATE_SIGNATURE = env("CRYPTOBOT_VALIDATE_SIGNATURE", "0") == "1"
CRYPTOBOT_INVOICE_EXPIRES_SECONDS = max(300, int(env("CRYPTOBOT_INVOICE_EXPIRES_SECONDS", "3600")))
CRYPTOBOT_DESCRIPTION_PREFIX = (
    env("CRYPTOBOT_DESCRIPTION_PREFIX", "BoxVolt VPN").strip() or "BoxVolt VPN"
)

# LZT Market (Merchant API)
LZT_ENABLED = env("LZT_ENABLED", "0") == "1"
LZT_API_TOKEN = env("LZT_API_TOKEN", "").strip()
LZT_API_BASE = env("LZT_API_BASE", "https://prod-api.lzt.market").strip().rstrip("/")
LZT_WEBHOOK_PATH = env("LZT_WEBHOOK_PATH", "/lzt/webhook").strip()
LZT_WEBHOOK_SECRET = env("LZT_WEBHOOK_SECRET", "").strip()
LZT_POLL_ENABLED = env("LZT_POLL_ENABLED", "1") == "1"
LZT_POLL_INTERVAL_SECONDS = max(10, int(env("LZT_POLL_INTERVAL_SECONDS", "30")))
LZT_CURRENCY = (env("LZT_CURRENCY", "rub").strip().lower() or "rub")
LZT_DESCRIPTION_PREFIX = env("LZT_DESCRIPTION_PREFIX", "BoxVolt VPN").strip() or "BoxVolt VPN"
try:
    LZT_MERCHANT_ID = int(env("LZT_MERCHANT_ID", "0").strip())
except ValueError:
    LZT_MERCHANT_ID = 0
LZT_MERCHANT_KEY = env("LZT_MERCHANT_KEY", "").strip()
PAYMENT_PROVIDER_PREFERRED = env("PAYMENT_PROVIDER", "").strip().lower()

APP_VERSION = env("APP_VERSION", "").strip()
UPDATE_NOTIFY_ON_START = env("UPDATE_NOTIFY_ON_START", "1") == "1"
UPDATE_NOTIFY_TEXT = env(
    "UPDATE_NOTIFY_TEXT",
    "🆕 Вышло новое обновление. Пропишите /start",
).strip()

# Bot settings
SUPPORT_CONTACT = env("SUPPORT_CONTACT", "@boxvolt_support")
START_PHOTO = env("START_PHOTO", "").strip()
BLACKLIST_TELEGRAM_IDS_RAW = env("BLACKLIST_TELEGRAM_IDS", "")
REQUIRE_CHANNEL_SUBSCRIPTION = env("REQUIRE_CHANNEL_SUBSCRIPTION", "1") == "1"
REQUIRED_CHANNEL_USERNAME = env("REQUIRED_CHANNEL_USERNAME", "@BoxVoltVPN").strip()
REQUIRED_CHANNEL_URL = env("REQUIRED_CHANNEL_URL", "").strip()
SUBSCRIPTION_PROMPT_COOLDOWN_SECONDS = max(
    1,
    int(env("SUBSCRIPTION_PROMPT_COOLDOWN_SECONDS", "15")),
)
TRIAL_ENABLED = env("TRIAL_ENABLED", "0") == "1"
TRIAL_DAYS = int(env("TRIAL_DAYS", "1"))
WEBAPP_PUBLIC_URL = env("WEBAPP_PUBLIC_URL", "")
WEBAPP_INITDATA_MAX_AGE_SECONDS = int(env("WEBAPP_INITDATA_MAX_AGE_SECONDS", "86400"))
PRICING_FILE = env("PRICING_FILE", str(BASE_DIR / "pricing.json"))
ADMIN_TELEGRAM_IDS_RAW = env("ADMIN_TELEGRAM_IDS", "")
ADMIN_NOTIFY_CHAT_IDS_RAW = env("ADMIN_NOTIFY_CHAT_IDS", "")
ADMIN_NOTIFY_TOPIC_ID_RAW = env("ADMIN_NOTIFY_TOPIC_ID", "").strip()
PAYMENT_PENDING_TTL_MINUTES = int(env("PAYMENT_PENDING_TTL_MINUTES", "15"))
PAYMENT_CLEANUP_INTERVAL_SECONDS = int(env("PAYMENT_CLEANUP_INTERVAL_SECONDS", "60"))
PENDING_ORDER_REMINDER_ENABLED = env("PENDING_ORDER_REMINDER_ENABLED", "1") == "1"
PENDING_ORDER_REMINDER_MINUTES = parse_int_list_env(
    env("PENDING_ORDER_REMINDER_MINUTES", "10,30"),
    min_value=1,
    max_value=1440,
    default=[10, 30],
    sort_desc=False,
)
PENDING_ORDER_REMINDER_INTERVAL_SECONDS = max(
    30,
    int(env("PENDING_ORDER_REMINDER_INTERVAL_SECONDS", "120")),
)
ORDER_CREATE_COOLDOWN_SECONDS = max(0, int(env("ORDER_CREATE_COOLDOWN_SECONDS", "45")))
ORDER_BURST_WINDOW_SECONDS = max(1, int(env("ORDER_BURST_WINDOW_SECONDS", "600")))
ORDER_BURST_MAX = max(1, int(env("ORDER_BURST_MAX", "8")))
TRIAL_REQUEST_COOLDOWN_SECONDS = max(0, int(env("TRIAL_REQUEST_COOLDOWN_SECONDS", "120")))
TRIAL_USERNAME_UNIQUE = env("TRIAL_USERNAME_UNIQUE", "1") == "1"
ADMIN_DAILY_REPORT_ENABLED = env("ADMIN_DAILY_REPORT_ENABLED", "1") == "1"
ADMIN_DAILY_REPORT_INTERVAL_SECONDS = max(3600, int(env("ADMIN_DAILY_REPORT_INTERVAL_SECONDS", "86400")))
ADMIN_DAILY_REPORT_CHECK_INTERVAL_SECONDS = max(
    60,
    int(env("ADMIN_DAILY_REPORT_CHECK_INTERVAL_SECONDS", "300")),
)
ADMIN_DAILY_REPORT_WINDOW_HOURS = max(1, int(env("ADMIN_DAILY_REPORT_WINDOW_HOURS", "24")))
PUBLIC_BASE_URL = env("PUBLIC_BASE_URL", "")
SUBSCRIPTION_PATH = env("SUBSCRIPTION_PATH", "/sub")
SUBSCRIPTION_SECRET = env(
    "SUBSCRIPTION_SECRET",
    DONATEPAY_WEBHOOK_SECRET or BOT_TOKEN,
)
SUBSCRIPTION_PROFILE_TITLE = env("SUBSCRIPTION_PROFILE_TITLE", "BoxVolt Технология 3.0")
SUBSCRIPTION_UPDATE_INTERVAL_HOURS = max(1, int(env("SUBSCRIPTION_UPDATE_INTERVAL_HOURS", "1")))
SUBSCRIPTION_REMINDER_ENABLED = env("SUBSCRIPTION_REMINDER_ENABLED", "1") == "1"
SUBSCRIPTION_REMINDER_HOURS = max(1, int(env("SUBSCRIPTION_REMINDER_HOURS", "24")))
SUBSCRIPTION_REMINDER_SCHEDULE_HOURS = parse_int_list_env(
    env("SUBSCRIPTION_REMINDER_SCHEDULE_HOURS", "24,6,1"),
    min_value=1,
    max_value=720,
    default=[SUBSCRIPTION_REMINDER_HOURS],
    sort_desc=False,
)
SUBSCRIPTION_REMINDER_INTERVAL_SECONDS = max(
    60,
    int(env("SUBSCRIPTION_REMINDER_INTERVAL_SECONDS", "300")),
)
SUPPORT_SLA_ENABLED = env("SUPPORT_SLA_ENABLED", "1") == "1"
SUPPORT_SLA_MINUTES = max(1, int(env("SUPPORT_SLA_MINUTES", "15")))
SUPPORT_SLA_CHECK_INTERVAL_SECONDS = max(
    60,
    int(env("SUPPORT_SLA_CHECK_INTERVAL_SECONDS", "120")),
)
SUPPORT_SLA_ALERT_LIMIT = max(1, int(env("SUPPORT_SLA_ALERT_LIMIT", "20")))
ANTIABUSE_FLAGS_ENABLED = env("ANTIABUSE_FLAGS_ENABLED", "1") == "1"
ANTIABUSE_FLAG_DEDUP_SECONDS = max(60, int(env("ANTIABUSE_FLAG_DEDUP_SECONDS", "1800")))
ANTIABUSE_FLAG_RETENTION_DAYS = max(1, int(env("ANTIABUSE_FLAG_RETENTION_DAYS", "30")))
AUTO_BACKUP_ENABLED = env("AUTO_BACKUP_ENABLED", "1") == "1"
AUTO_BACKUP_INTERVAL_SECONDS = max(3600, int(env("AUTO_BACKUP_INTERVAL_SECONDS", "86400")))
AUTO_BACKUP_CHECK_INTERVAL_SECONDS = max(60, int(env("AUTO_BACKUP_CHECK_INTERVAL_SECONDS", "300")))
AUTO_BACKUP_KEEP_FILES = max(1, int(env("AUTO_BACKUP_KEEP_FILES", "14")))
AUTO_BACKUP_DIR = env("AUTO_BACKUP_DIR", str(BASE_DIR / "backups")).strip()
AUTO_BACKUP_TARGETS = parse_csv_env(
    env("AUTO_BACKUP_TARGETS", "users.db,pricing.json"),
    default=["users.db", "pricing.json"],
)
SERVICE_MONITOR_ENABLED = env("SERVICE_MONITOR_ENABLED", "1") == "1"
SERVICE_MONITOR_SERVICES = parse_csv_env(
    env("SERVICE_MONITOR_SERVICES", "boxvolt-bot,x-ui,nginx"),
    default=["boxvolt-bot", "x-ui", "nginx"],
)
SERVICE_MONITOR_INTERVAL_SECONDS = max(
    30,
    int(env("SERVICE_MONITOR_INTERVAL_SECONDS", "90")),
)
SERVICE_MONITOR_NOTIFY_RECOVERY = env("SERVICE_MONITOR_NOTIFY_RECOVERY", "1") == "1"
BOT_USERNAME = env("BOT_USERNAME", "").strip().lstrip("@")
REFERRAL_ENABLED = env("REFERRAL_ENABLED", "1") == "1"
REFERRAL_REWARD_DAYS = max(1, int(env("REFERRAL_REWARD_DAYS", "3")))
REFERRAL_MIN_PLAN_DAYS = max(1, int(env("REFERRAL_MIN_PLAN_DAYS", "14")))
REFERRAL_LINK_CODE_LENGTH = max(6, min(24, int(env("REFERRAL_LINK_CODE_LENGTH", "8"))))
REFERRAL_CAMPAIGN_LABEL_MAX = 64

SECONDARY_PAYMENT_ENABLED = env("SECONDARY_PAYMENT_ENABLED", "0") == "1"
SECONDARY_PAYMENT_LABEL = env("SECONDARY_PAYMENT_LABEL", "Резервный метод").strip() or "Резервный метод"
SECONDARY_PAYMENT_URL = env("SECONDARY_PAYMENT_URL", "").strip()
SECONDARY_PROVIDER = "secondary"

ROUTE_CIS_LANG_CODES = {
    item.lower()
    for item in parse_csv_env(env("ROUTE_CIS_LANG_CODES", "ru,uk,be,kk,ky,uz,tg,tk,hy,az"), default=["ru"])
}
ROUTE_RESERVE_INBOUND_ID = max(0, int(env("ROUTE_RESERVE_INBOUND_ID", str(SPEED_INBOUND_ID))))
ROUTE_RESERVE_NAME = env("ROUTE_RESERVE_NAME", "Резервный").strip() or "Резервный"

LOYALTY_ENABLED = env("LOYALTY_ENABLED", "1") == "1"
LOYALTY_EVERY_PAID = max(2, int(env("LOYALTY_EVERY_PAID", "5")))
LOYALTY_BONUS_DAYS = max(1, int(env("LOYALTY_BONUS_DAYS", "3")))

NPS_ENABLED = env("NPS_ENABLED", "1") == "1"
NPS_AFTER_DAYS = max(1, int(env("NPS_AFTER_DAYS", "3")))
NPS_CHECK_INTERVAL_SECONDS = max(300, int(env("NPS_CHECK_INTERVAL_SECONDS", "3600")))
NPS_PROMPT_COOLDOWN_DAYS = max(7, int(env("NPS_PROMPT_COOLDOWN_DAYS", "30")))
APP_LINK_HAPP_WINDOWS = env(
    "APP_LINK_HAPP_WINDOWS",
    "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe",
).strip()
APP_LINK_HAPP_ANDROID = env(
    "APP_LINK_HAPP_ANDROID",
    "https://play.google.com/store/apps/details?id=com.happproxy",
).strip()
APP_LINK_HAPP_IOS = env(
    "APP_LINK_HAPP_IOS",
    "https://apps.apple.com/us/app/happ-proxy-utility/id6504287215",
).strip()
APP_LINK_HAPP_MACOS = env(
    "APP_LINK_HAPP_MACOS",
    "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ.macOS.universal.dmg",
).strip()
APP_LINK_V2RAYTUN_ANDROID = env(
    "APP_LINK_V2RAYTUN_ANDROID",
    "https://play.google.com/store/apps/details?id=com.v2raytun.android",
).strip()
APP_LINK_V2RAYTUN_IOS = env(
    "APP_LINK_V2RAYTUN_IOS",
    "https://apps.apple.com/app/v2raytun/id6476628951",
).strip()

# Web server for webhook
WEBHOOK_HOST = env("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT = int(env("WEBHOOK_PORT", "8080"))

DEFAULT_PLANS: dict[str, Plan] = {
    "w1": Plan(
        code="w1",
        title="7 дней",
        days=7,
        amount_rub=35,
    ),
    "m1": Plan(
        code="m1",
        title="30 дней",
        days=30,
        amount_rub=110,
    ),
    "m3": Plan(
        code="m3",
        title="90 дней",
        days=90,
        amount_rub=300,
    ),
    "y1": Plan(
        code="y1",
        title="365 дней",
        days=365,
        amount_rub=1200,
    ),
}

GUIDES: dict[str, tuple[str, str]] = {
    "android_happ": (
        "🤖 Android • Happ",
        "1. Установите Happ из Google Play.\n"
        "2. Откройте приложение и выберите импорт Subscription URL.\n"
        "3. Вставьте URL-подписку из бота (не обычный одноразовый ключ).\n"
        "4. Сохраните профиль и нажмите Connect.",
    ),
    "android_v2raytun": (
        "🤖 Android • V2rayTun",
        "1. Установите V2rayTun.\n"
        "2. Нажмите + и выберите импорт URL.\n"
        "3. Вставьте ключ из бота и сохраните профиль.\n"
        "4. Активируйте туннель кнопкой Start.",
    ),
    "ios_happ": (
        "🍏 iOS • Happ",
        "1. Установите Happ из App Store.\n"
        "2. Разрешите VPN-конфигурации в iOS при первом запуске.\n"
        "3. Импортируйте URL-подписку из бота (Subscription URL).\n"
        "4. Нажмите Connect.",
    ),
    "windows_v2raytun": (
        "🪟 Windows • V2rayTun",
        "1. Установите V2rayTun для Windows.\n"
        "2. Импортируйте ключ через Add profile -> Import URL.\n"
        "3. Выберите профиль BoxVolt и нажмите Start.\n"
        "4. При необходимости включите запуск от администратора.",
    ),
    "macos_happ": (
        "🍎 macOS • Happ",
        "1. Установите Happ для macOS.\n"
        "2. Импортируйте URL-подписку из бота (Subscription URL).\n"
        "3. Разрешите создание VPN-профиля в настройках системы.\n"
        "4. Запустите подключение.",
    ),
    "linux_v2raytun": (
        "🐧 Linux • V2rayTun",
        "1. Установите V2rayTun для Linux (deb/rpm/appimage).\n"
        "2. Импортируйте VLESS-ссылку через URL.\n"
        "3. Запустите профиль и проверьте маршрут трафика.\n"
        "4. Если нужен TUN, убедитесь что есть права root/cap_net_admin.",
    ),
}

ORDER_ID_RE = re.compile(r"\bBV-[A-Z0-9_-]{6,80}\b", re.IGNORECASE)
START_ORDER_RE = re.compile(r"^(?:pay|order|oid)[_-]?(BV-[A-Z0-9_-]{6,80})$", re.IGNORECASE)
PROMO_CODE_RE = re.compile(r"^[A-Z0-9_-]{3,32}$")
REF_START_RE = re.compile(r"^(?:ref|r)[_-]?(\d{5,20})$", re.IGNORECASE)
ORDER_STATUS_MARKER = "\n\n📌 Статус заказа:\n"
PROCESS_LOCK = asyncio.Lock()
WEBAPP_TEMPLATE_PATH = BASE_DIR / "webapp" / "index.html"
DONATEPAY_PROVIDER = "donatepay"
CRYPTOBOT_PROVIDER = "cryptobot"
LZT_PROVIDER = "lzt"
DONATEPAY_LAST_TRANSACTION_ID = 0
PRICING_CACHE: dict[str, Any] | None = None
PRICING_CACHE_MTIME: float | None = None
REALITY_PROFILE_CACHE: dict[str, str] | None = None
REALITY_PROFILE_CACHE_AT: dt.datetime | None = None
BOT_PUBLIC_USERNAME_CACHE = BOT_USERNAME
APP_RUNTIME_VERSION_CACHE = ""
ADMIN_TELEGRAM_IDS = {
    int(value.strip())
    for value in ADMIN_TELEGRAM_IDS_RAW.split(",")
    if value.strip().isdigit()
}
ADMIN_NOTIFY_CHAT_IDS = {
    int(value.strip())
    for value in ADMIN_NOTIFY_CHAT_IDS_RAW.split(",")
    if re.fullmatch(r"-?\d+", value.strip())
}
BLACKLIST_TELEGRAM_IDS = {
    int(value.strip())
    for value in BLACKLIST_TELEGRAM_IDS_RAW.split(",")
    if re.fullmatch(r"-?\d+", value.strip())
}
ADMIN_NOTIFY_TOPIC_ID = int(ADMIN_NOTIFY_TOPIC_ID_RAW) if re.fullmatch(r"\d+", ADMIN_NOTIFY_TOPIC_ID_RAW) else 0
SUPPORT_WAITING_USERS: set[int] = set()
ADMIN_REPLY_TICKET_BY_ADMIN: dict[int, int] = {}
USER_ACTIVE_TICKET_CHAT_BY_USER: dict[int, int] = {}
USER_TICKET_CHAT_DISABLED: set[int] = set()
SUPPORT_TICKET_ADMIN_MESSAGE_IDS: dict[int, dict[int, int]] = {}
SUPPORT_USER_MEDIA_GROUP_BUFFER: dict[tuple[int, int, str], dict[str, Any]] = {}
SUPPORT_APPEND_BUTTON = "✍️ Дополнить тикет"
SUPPORT_EXIT_BUTTON = "✅ Выйти из тикета"
PROMO_WAITING_USERS: set[int] = set()
NPS_COMMENT_WAITING_USERS: set[int] = set()
SUBSCRIPTION_PROMPT_LAST_SENT_AT: dict[int, dt.datetime] = {}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def init_db() -> None:
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            language_code TEXT,
            subscription_end TEXT,
            vless_uuid TEXT,
            trial_used INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )

    if not _column_exists(conn, "users", "vless_uuid"):
        cursor.execute("ALTER TABLE users ADD COLUMN vless_uuid TEXT")
    if not _column_exists(conn, "users", "language_code"):
        cursor.execute("ALTER TABLE users ADD COLUMN language_code TEXT")
    if not _column_exists(conn, "users", "trial_used"):
        cursor.execute("ALTER TABLE users ADD COLUMN trial_used INTEGER DEFAULT 0")
    if not _column_exists(conn, "users", "created_at"):
        cursor.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
    cursor.execute(
        """
        UPDATE users
        SET created_at = COALESCE(NULLIF(created_at, ''), ?)
        WHERE created_at IS NULL OR created_at = ''
        """,
        (now_str(),),
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT UNIQUE NOT NULL,
            telegram_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            amount_rub REAL NOT NULL,
            days INTEGER NOT NULL,
            plan_code TEXT,
            base_amount_rub REAL,
            promo_code TEXT,
            promo_discount_rub INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            paid_at TEXT,
            raw_payload TEXT,
            FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments (telegram_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (status)")

    if not _column_exists(conn, "payments", "plan_code"):
        cursor.execute("ALTER TABLE payments ADD COLUMN plan_code TEXT")
    if not _column_exists(conn, "payments", "base_amount_rub"):
        cursor.execute("ALTER TABLE payments ADD COLUMN base_amount_rub REAL")
    if not _column_exists(conn, "payments", "promo_code"):
        cursor.execute("ALTER TABLE payments ADD COLUMN promo_code TEXT")
    if not _column_exists(conn, "payments", "promo_discount_rub"):
        cursor.execute("ALTER TABLE payments ADD COLUMN promo_discount_rub INTEGER NOT NULL DEFAULT 0")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            discount_rub INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            max_activations INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            created_by INTEGER
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_activations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            activated_at TEXT NOT NULL,
            used_at TEXT,
            used_order_id TEXT,
            UNIQUE(code, telegram_id),
            FOREIGN KEY (code) REFERENCES promo_codes (code)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_code ON promo_activations (code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_tg ON promo_activations (telegram_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_used ON promo_activations (used_at)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trial_claims (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            claimed_at TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklisted_users (
            telegram_id INTEGER PRIMARY KEY,
            reason TEXT,
            created_at TEXT NOT NULL,
            created_by INTEGER
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_blacklisted_users_created_at ON blacklisted_users (created_at)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            invited_telegram_id INTEGER PRIMARY KEY,
            referrer_telegram_id INTEGER NOT NULL,
            referral_code TEXT,
            invited_username TEXT,
            linked_at TEXT NOT NULL
        )
        """
    )
    if not _column_exists(conn, "referrals", "referral_code"):
        cursor.execute("ALTER TABLE referrals ADD COLUMN referral_code TEXT")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_links (
            code TEXT PRIMARY KEY,
            referrer_telegram_id INTEGER NOT NULL,
            label TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT UNIQUE NOT NULL,
            referrer_telegram_id INTEGER NOT NULL,
            invited_telegram_id INTEGER NOT NULL,
            paid_days INTEGER NOT NULL,
            reward_days INTEGER NOT NULL,
            rewarded_at TEXT NOT NULL
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals (referrer_telegram_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_referrals_code ON referrals (referral_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_referral_links_referrer ON referral_links (referrer_telegram_id)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_referral_rewards_referrer ON referral_rewards (referrer_telegram_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_referral_rewards_invited ON referral_rewards (invited_telegram_id)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS loyalty_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT UNIQUE NOT NULL,
            telegram_id INTEGER NOT NULL,
            paid_count INTEGER NOT NULL,
            reward_days INTEGER NOT NULL,
            rewarded_at TEXT NOT NULL
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_loyalty_rewards_tg ON loyalty_rewards (telegram_id)")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS nps_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            comment TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_feedback_tg ON nps_feedback (telegram_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_feedback_created ON nps_feedback (created_at)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS antiabuse_flags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            flag_type TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            seen_count INTEGER NOT NULL DEFAULT 1,
            resolved INTEGER NOT NULL DEFAULT 0,
            resolved_at TEXT,
            resolved_by INTEGER
        )
        """
    )
    if not _column_exists(conn, "antiabuse_flags", "last_seen_at"):
        cursor.execute("ALTER TABLE antiabuse_flags ADD COLUMN last_seen_at TEXT")
    if not _column_exists(conn, "antiabuse_flags", "seen_count"):
        cursor.execute("ALTER TABLE antiabuse_flags ADD COLUMN seen_count INTEGER NOT NULL DEFAULT 1")
    if not _column_exists(conn, "antiabuse_flags", "resolved"):
        cursor.execute("ALTER TABLE antiabuse_flags ADD COLUMN resolved INTEGER NOT NULL DEFAULT 0")
    if not _column_exists(conn, "antiabuse_flags", "resolved_at"):
        cursor.execute("ALTER TABLE antiabuse_flags ADD COLUMN resolved_at TEXT")
    if not _column_exists(conn, "antiabuse_flags", "resolved_by"):
        cursor.execute("ALTER TABLE antiabuse_flags ADD COLUMN resolved_by INTEGER")
    cursor.execute(
        """
        UPDATE antiabuse_flags
        SET last_seen_at = COALESCE(NULLIF(last_seen_at, ''), created_at, ?)
        WHERE last_seen_at IS NULL OR last_seen_at = ''
        """,
        (now_str(),),
    )
    cursor.execute(
        """
        UPDATE antiabuse_flags
        SET seen_count = COALESCE(seen_count, 1)
        WHERE seen_count IS NULL OR seen_count < 1
        """
    )
    cursor.execute(
        """
        UPDATE antiabuse_flags
        SET resolved = COALESCE(resolved, 0)
        WHERE resolved IS NULL
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_antiabuse_flags_tg ON antiabuse_flags (telegram_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_antiabuse_flags_created ON antiabuse_flags (created_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_antiabuse_flags_resolved ON antiabuse_flags (resolved)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT,
            initial_message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            assigned_admin_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            taken_at TEXT,
            closed_at TEXT,
            closed_by TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS support_ticket_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL,
            sender_role TEXT NOT NULL,
            sender_id INTEGER,
            message TEXT NOT NULL,
            media_kind TEXT NOT NULL DEFAULT 'text',
            media_file_id TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (ticket_id) REFERENCES support_tickets (id)
        )
        """
    )
    if not _column_exists(conn, "support_tickets", "updated_at"):
        cursor.execute("ALTER TABLE support_tickets ADD COLUMN updated_at TEXT")
    if not _column_exists(conn, "support_tickets", "closed_by"):
        cursor.execute("ALTER TABLE support_tickets ADD COLUMN closed_by TEXT")
    if not _column_exists(conn, "support_ticket_messages", "media_kind"):
        cursor.execute("ALTER TABLE support_ticket_messages ADD COLUMN media_kind TEXT DEFAULT 'text'")
    if not _column_exists(conn, "support_ticket_messages", "media_file_id"):
        cursor.execute("ALTER TABLE support_ticket_messages ADD COLUMN media_file_id TEXT")
    cursor.execute(
        """
        UPDATE support_tickets
        SET updated_at = COALESCE(NULLIF(updated_at, ''), NULLIF(closed_at, ''), NULLIF(taken_at, ''), created_at, ?)
        WHERE updated_at IS NULL OR updated_at = ''
        """,
        (now_str(),),
    )
    cursor.execute(
        """
        UPDATE support_tickets
        SET closed_by = COALESCE(NULLIF(closed_by, ''), CASE WHEN status = 'closed' THEN 'admin' ELSE NULL END)
        WHERE closed_by IS NULL OR closed_by = ''
        """
    )
    cursor.execute(
        """
        UPDATE support_ticket_messages
        SET media_kind = COALESCE(NULLIF(media_kind, ''), 'text')
        WHERE media_kind IS NULL OR media_kind = ''
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_user ON support_tickets (telegram_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON support_tickets (status)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_support_ticket_messages_ticket ON support_ticket_messages (ticket_id)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    conn.commit()
    conn.close()


def now_str() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_date(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def has_active_subscription(subscription_end: str | None) -> bool:
    expiry = parse_date(subscription_end)
    return bool(expiry and expiry > dt.datetime.now())


def format_subscription_remaining(subscription_end: str | None) -> str:
    expiry = parse_date(subscription_end)
    if not expiry:
        return "нет активной"

    delta = expiry - dt.datetime.now()
    if delta.total_seconds() <= 0:
        return "истекла"

    total_seconds = int(delta.total_seconds())
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60

    if days > 0:
        return f"{days} дн. {hours} ч."
    if hours > 0:
        return f"{hours} ч. {minutes} мин."
    return f"{max(1, minutes)} мин."


def day_word(value: int) -> str:
    amount = abs(int(value))
    mod10 = amount % 10
    mod100 = amount % 100
    if mod10 == 1 and mod100 != 11:
        return "день"
    if mod10 in {2, 3, 4} and mod100 not in {12, 13, 14}:
        return "дня"
    return "дней"


def build_rules_text() -> str:
    return (
        "📜 Правила BoxVolt VPN\n\n"
        "1. Используйте сервис только в законных целях.\n"
        "2. Оплата засчитывается при сумме не ниже тарифа (комиссия сверху допустима).\n"
        "3. Тест предоставляется 1 раз и только до первой оплаты.\n"
        "4. Реферальный бонус начисляется после оплаты приглашенным тарифа от 14 дней.\n"
        "5. Передача доступа третьим лицам и фрод-злоупотребления запрещены.\n"
        "6. При нарушениях доступ может быть ограничен без компенсации.\n"
        "7. По вопросам: "
        f"{SUPPORT_CONTACT}"
    )


def payment_ttl() -> dt.timedelta:
    return dt.timedelta(minutes=max(1, PAYMENT_PENDING_TTL_MINUTES))


def payment_expires_at(created_at: str | None) -> dt.datetime | None:
    created = parse_date(created_at)
    if not created:
        return None
    return created + payment_ttl()


def is_payment_expired(created_at: str | None) -> bool:
    expires_at = payment_expires_at(created_at)
    return bool(expires_at and expires_at <= dt.datetime.now())


def payment_expires_at_str(created_at: str | None) -> str | None:
    expires_at = payment_expires_at(created_at)
    if not expires_at:
        return None
    return expires_at.strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return default


def _clamp(value: int, lower: int, upper: int) -> int:
    return max(lower, min(upper, value))


def pricing_path() -> Path:
    return Path(PRICING_FILE)


def default_pricing_payload() -> dict[str, Any]:
    return {
        "global_discount_percent": 0,
        "sale_title": "",
        "sale_message": "",
        "plans": [
            {
                "code": plan.code,
                "title": plan.title,
                "days": plan.days,
                "amount_rub": plan.amount_rub,
                "discount_percent": 0,
            }
            for plan in DEFAULT_PLANS.values()
        ],
    }


def ensure_pricing_file_exists() -> None:
    path = pricing_path()
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(default_pricing_payload(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_pricing_config() -> dict[str, Any]:
    global PRICING_CACHE, PRICING_CACHE_MTIME

    ensure_pricing_file_exists()
    path = pricing_path()

    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = None

    if PRICING_CACHE is not None and PRICING_CACHE_MTIME == mtime:
        return PRICING_CACHE

    payload: dict[str, Any]
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        payload = raw if isinstance(raw, dict) else default_pricing_payload()
    except (OSError, json.JSONDecodeError):
        payload = default_pricing_payload()

    if not isinstance(payload.get("plans"), list):
        payload["plans"] = default_pricing_payload()["plans"]

    PRICING_CACHE = payload
    PRICING_CACHE_MTIME = mtime
    return payload


def normalize_pricing_payload(payload: Any) -> tuple[dict[str, Any] | None, str]:
    if not isinstance(payload, dict):
        return None, "pricing_payload_invalid"

    global_discount = _clamp(_safe_int(payload.get("global_discount_percent"), 0), 0, 90)
    sale_title = str(payload.get("sale_title") or "").strip()[:120]
    sale_message = str(payload.get("sale_message") or "").strip()[:2000]

    plans_raw = payload.get("plans")
    if not isinstance(plans_raw, list):
        return None, "plans_invalid"

    plans: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for item in plans_raw:
        if not isinstance(item, dict):
            continue

        code = str(item.get("code") or "").strip().lower()
        title = str(item.get("title") or "").strip()
        if not code or not title:
            continue
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", code):
            continue
        if code in seen_codes:
            continue
        seen_codes.add(code)

        days = _clamp(_safe_int(item.get("days"), 0), 1, 3650)
        amount_rub = _clamp(_safe_int(item.get("amount_rub"), 0), 1, 100000)
        discount_percent = _clamp(_safe_int(item.get("discount_percent"), 0), 0, 90)

        plans.append(
            {
                "code": code,
                "title": title[:80],
                "days": days,
                "amount_rub": amount_rub,
                "discount_percent": discount_percent,
            }
        )

    if not plans:
        return None, "plans_empty"

    normalized = {
        "global_discount_percent": global_discount,
        "sale_title": sale_title,
        "sale_message": sale_message,
        "plans": plans,
    }
    return normalized, "ok"


def get_editable_pricing_config() -> dict[str, Any]:
    normalized, _ = normalize_pricing_payload(load_pricing_config())
    if normalized:
        return normalized
    fallback, _ = normalize_pricing_payload(default_pricing_payload())
    return fallback or default_pricing_payload()


def save_pricing_config(payload: dict[str, Any]) -> None:
    global PRICING_CACHE, PRICING_CACHE_MTIME

    path = pricing_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    PRICING_CACHE = payload
    try:
        PRICING_CACHE_MTIME = path.stat().st_mtime
    except OSError:
        PRICING_CACHE_MTIME = None


def get_active_plans() -> dict[str, Plan]:
    config = load_pricing_config()
    global_discount = _clamp(_safe_int(config.get("global_discount_percent"), 0), 0, 90)
    plans: dict[str, Plan] = {}

    plans_raw = config.get("plans")
    if isinstance(plans_raw, list):
        for item in plans_raw:
            if not isinstance(item, dict):
                continue

            code = str(item.get("code", "")).strip()
            title = str(item.get("title", "")).strip()
            if not code or not title:
                continue

            days = _safe_int(item.get("days"), 0)
            amount_base = _safe_int(item.get("amount_rub"), 0)
            plan_discount = _clamp(_safe_int(item.get("discount_percent"), 0), 0, 90)
            total_discount = _clamp(global_discount + plan_discount, 0, 90)

            if days <= 0 or amount_base <= 0:
                continue

            amount_final = max(1, round(amount_base * (100 - total_discount) / 100))
            plans[code] = Plan(
                code=code,
                title=title,
                days=days,
                amount_rub=amount_final,
                base_amount_rub=amount_base,
                discount_percent=total_discount,
            )

    if plans:
        return plans

    # Fallback: если pricing.json поврежден, используем базовые планы из env.
    for plan in DEFAULT_PLANS.values():
        amount_final = max(1, round(plan.amount_rub * (100 - global_discount) / 100))
        plans[plan.code] = Plan(
            code=plan.code,
            title=plan.title,
            days=plan.days,
            amount_rub=amount_final,
            base_amount_rub=plan.amount_rub,
            discount_percent=global_discount,
        )
    return plans


def get_sale_text() -> str | None:
    config = load_pricing_config()
    plans = get_active_plans()

    sale_title = str(config.get("sale_title") or "").strip()
    sale_message = str(config.get("sale_message") or "").strip()
    max_discount = max((plan.discount_percent for plan in plans.values()), default=0)

    if max_discount <= 0 and not sale_message:
        return None

    lines = ["🔥 Акция BoxVolt VPN"]
    if sale_title:
        lines.append(sale_title)
    if max_discount > 0:
        lines.append(f"Скидка до {max_discount}% на выбранные тарифы.")
    if sale_message:
        lines.append(sale_message)
    return "\n".join(lines)


def plan_line(plan: Plan) -> str:
    if plan.discount_percent > 0 and plan.base_amount_rub > plan.amount_rub:
        return f"{plan.title} • {plan.amount_rub} ₽ (-{plan.discount_percent}%, было {plan.base_amount_rub} ₽)"
    return f"{plan.title} • {plan.amount_rub} ₽"


def get_quick_renew_plan_30_days() -> Plan | None:
    plans = get_active_plans()
    m1 = plans.get("m1")
    if m1 and int(m1.days) == 30:
        return m1
    for plan in plans.values():
        if int(plan.days) == 30:
            return plan
    return None


def get_plan_by_days(days: int) -> Plan | None:
    target = max(1, int(days))
    plans = get_active_plans()
    preferred_by_days = {
        7: "w1",
        30: "m1",
        90: "m3",
        365: "y1",
    }
    preferred_code = preferred_by_days.get(target)
    if preferred_code:
        preferred = plans.get(preferred_code)
        if preferred and int(preferred.days) == target:
            return preferred
    for plan in plans.values():
        if int(plan.days) == target:
            return plan
    return None


def get_last_paid_plan_for_user(telegram_id: int) -> Plan | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT plan_code, days
        FROM payments
        WHERE telegram_id = ?
          AND status = 'paid'
        ORDER BY COALESCE(paid_at, created_at) DESC, id DESC
        LIMIT 1
        """,
        (int(telegram_id),),
    ).fetchone()
    conn.close()
    if not row:
        return None

    plans = get_active_plans()
    plan_code = str(row["plan_code"] or "").strip()
    if plan_code:
        by_code = plans.get(plan_code)
        if by_code:
            return by_code

    return get_plan_by_days(int(row["days"] or 0))


def resolve_plan_from_payment_row(payment: sqlite3.Row | dict[str, Any] | None) -> Plan | None:
    if not payment:
        return None
    plans = get_active_plans()
    plan_code = str(payment["plan_code"] or "").strip()
    if plan_code:
        by_code = plans.get(plan_code)
        if by_code:
            return by_code
    return get_plan_by_days(int(payment["days"] or 0))


def payment_identity_values(
    telegram_id: int | None = None,
    username: str | None = None,
) -> tuple[str | None, str | None]:
    username_clean = str(username or "").strip().lstrip("@")
    payer_tag: str | None = None
    if username_clean:
        payer_tag = username_clean
    elif telegram_id is not None and int(telegram_id) > 0:
        payer_tag = str(int(telegram_id))
    return payer_tag, username_clean or None


def is_admin_user(telegram_id: int) -> bool:
    return bool(ADMIN_TELEGRAM_IDS) and telegram_id in ADMIN_TELEGRAM_IDS


def required_channel_display() -> str:
    channel = REQUIRED_CHANNEL_USERNAME.strip()
    if not channel:
        return "@BoxVoltVPN"
    if channel.startswith("https://t.me/"):
        return f"@{channel.split('/')[-1]}"
    if channel.startswith("@"):
        return channel
    return f"@{channel}"


def required_channel_join_url() -> str:
    if REQUIRED_CHANNEL_URL:
        return REQUIRED_CHANNEL_URL
    channel = REQUIRED_CHANNEL_USERNAME.strip()
    if not channel:
        return "https://t.me/BoxVoltVPN"
    if channel.startswith("https://t.me/"):
        return channel
    return f"https://t.me/{channel.lstrip('@')}"


def build_subscription_required_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Подписаться",
                    url=required_channel_join_url(),
                )
            ],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="subcheck:refresh")],
        ]
    )


def should_send_subscription_prompt(telegram_id: int) -> bool:
    if SUBSCRIPTION_PROMPT_COOLDOWN_SECONDS <= 0:
        return True
    now = dt.datetime.utcnow()
    last = SUBSCRIPTION_PROMPT_LAST_SENT_AT.get(telegram_id)
    if last and (now - last).total_seconds() < SUBSCRIPTION_PROMPT_COOLDOWN_SECONDS:
        return False
    SUBSCRIPTION_PROMPT_LAST_SENT_AT[telegram_id] = now
    return True


async def user_has_required_channel_subscription(telegram_id: int) -> bool:
    if not REQUIRE_CHANNEL_SUBSCRIPTION or not REQUIRED_CHANNEL_USERNAME:
        return True

    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_USERNAME, telegram_id)
    except Exception as exc:  # noqa: BLE001
        print(f"[subscription] Failed to check user {telegram_id}: {exc}")
        return False

    status = str(getattr(member, "status", "")).lower()
    if status in {"member", "administrator", "creator"}:
        return True
    return bool(getattr(member, "is_member", False))


async def check_required_channel_subscription(telegram_id: int) -> tuple[bool, str | None]:
    if not REQUIRE_CHANNEL_SUBSCRIPTION or not REQUIRED_CHANNEL_USERNAME:
        return True, None

    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_USERNAME, telegram_id)
    except Exception as exc:  # noqa: BLE001
        message = str(exc).lower()
        print(f"[subscription] Failed to check user {telegram_id}: {exc}")
        if "member list is inaccessible" in message:
            return False, "member_list_inaccessible"
        return False, "check_failed"

    status = str(getattr(member, "status", "")).lower()
    if status in {"member", "administrator", "creator"}:
        return True, None
    if bool(getattr(member, "is_member", False)):
        return True, None
    return False, "not_subscribed"


async def send_subscription_required_prompt(
    message: Message,
    telegram_id: int | None = None,
    check_reason: str | None = None,
) -> None:
    user_id = telegram_id or (message.from_user.id if message.from_user else 0)
    if user_id and not should_send_subscription_prompt(user_id):
        return
    extra = ""
    if check_reason == "member_list_inaccessible":
        extra = "\n\n⚠️ Сейчас бот не имеет доступа к списку подписчиков канала."
    await message.answer(
        "📢 Подпишитесь на наш информационный канал, чтобы продолжить.\n"
        f"Канал: {required_channel_display()}{extra}",
        reply_markup=build_subscription_required_keyboard(),
    )


class SubscriptionRequiredMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: dict[str, Any],
    ) -> Any:
        if not REQUIRE_CHANNEL_SUBSCRIPTION:
            return await handler(event, data)

        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)

        telegram_id = int(user.id)
        upsert_user(telegram_id, getattr(user, "username", None), getattr(user, "language_code", None))
        if not is_admin_user(telegram_id) and is_user_blacklisted(telegram_id):
            block_text = "⛔ Доступ к боту ограничен. Обратитесь в поддержку."
            if isinstance(event, Message):
                text = str(event.text or "").strip()
                text_lower = text.lower()
                is_support_command = text_lower == "/support" or text_lower.startswith("/support@")
                is_support_button = text_lower in {
                    "🛟 поддержка",
                    "поддержка",
                    SUPPORT_APPEND_BUTTON.lower(),
                    SUPPORT_EXIT_BUTTON.lower(),
                }
                document = getattr(event, "document", None)
                is_image_document = bool(
                    document and str(getattr(document, "mime_type", "")).lower().startswith("image/")
                )
                has_image_payload = bool(getattr(event, "photo", None)) or is_image_document
                has_open_ticket = get_latest_open_support_ticket_for_user(telegram_id) is not None
                is_support_ticket_input = (telegram_id in SUPPORT_WAITING_USERS or has_open_ticket) and (
                    (text and not text.startswith("/")) or text_lower in {"/cancel", "отмена"} or has_image_payload
                )
                if is_support_command or is_support_button or is_support_ticket_input:
                    return await handler(event, data)
                log_suspicious_flag(
                    telegram_id,
                    "blacklist_access_attempt",
                    f"message:{text_lower[:120]}",
                )
                await event.answer(block_text)
                return None
            if isinstance(event, CallbackQuery):
                log_suspicious_flag(
                    telegram_id,
                    "blacklist_access_attempt",
                    f"callback:{str(event.data or '')[:120]}",
                )
                await event.answer("⛔ Доступ ограничен", show_alert=True)
                return None
            return None

        if isinstance(event, CallbackQuery):
            callback_data = str(event.data or "")
            if callback_data.startswith("subcheck:"):
                return await handler(event, data)
        if isinstance(event, Message):
            text = str(event.text or "").strip().lower()
            if text.startswith("/start"):
                return await handler(event, data)

        subscribed, reason = await check_required_channel_subscription(telegram_id)
        if subscribed:
            return await handler(event, data)

        if isinstance(event, CallbackQuery):
            if reason == "member_list_inaccessible":
                await event.answer(
                    "Бот не имеет доступа к списку подписчиков канала. "
                    "Добавьте бота админом канала и попробуйте снова.",
                    show_alert=True,
                )
            else:
                await event.answer("Подпишитесь на канал, чтобы продолжить.", show_alert=True)
            if event.message:
                await send_subscription_required_prompt(event.message, telegram_id, reason)
            return None

        if isinstance(event, Message):
            await send_subscription_required_prompt(event, check_reason=reason)
            return None

        return None


dp.message.middleware(SubscriptionRequiredMiddleware())
dp.callback_query.middleware(SubscriptionRequiredMiddleware())


def command_args(message: Message) -> str:
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


async def ensure_admin(message: Message) -> bool:
    if is_admin_user(message.from_user.id):
        return True

    if not ADMIN_TELEGRAM_IDS:
        await message.answer(
            "⚠️ Админ-команды отключены: заполните `ADMIN_TELEGRAM_IDS` в .env."
        )
    else:
        await message.answer("⛔ Эта команда доступна только администратору.")
    return False


async def broadcast_text(text: str) -> tuple[int, int]:
    sent = 0
    failed = 0
    for telegram_id in get_all_user_ids():
        try:
            await bot.send_message(
                telegram_id,
                text,
                reply_markup=build_main_keyboard(telegram_id),
            )
            sent += 1
        except Exception:  # noqa: BLE001
            failed += 1
        await asyncio.sleep(0.03)
    return sent, failed


def upsert_user(telegram_id: int, username: str | None, language_code: str | None = None) -> None:
    lang = str(language_code or "").strip().lower()[:12] or None
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username, language_code, created_at) VALUES (?, ?, ?, ?)",
        (telegram_id, username, lang, now_str()),
    )
    conn.execute(
        """
        UPDATE users
        SET username = COALESCE(?, username),
            language_code = COALESCE(?, language_code),
            created_at = COALESCE(NULLIF(created_at, ''), ?)
        WHERE telegram_id = ?
        """,
        (username, lang, now_str(), telegram_id),
    )
    conn.commit()
    conn.close()


def get_user(telegram_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT telegram_id, username, language_code, subscription_end, vless_uuid, trial_used "
        "FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return row


def user_route_mode_meta_key(telegram_id: int) -> str:
    return f"user_route_mode:{int(telegram_id)}"


def get_user_route_mode(telegram_id: int) -> str:
    raw = str(get_app_meta(user_route_mode_meta_key(telegram_id)) or "").strip().lower()
    if raw in {"main", "reserve", "auto"}:
        return raw
    return "auto"


def set_user_route_mode(telegram_id: int, mode: str) -> None:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"main", "reserve", "auto"}:
        normalized = "auto"
    set_app_meta(user_route_mode_meta_key(telegram_id), normalized)


def user_is_cis_preferred(user: sqlite3.Row | None) -> bool:
    if not user:
        return False
    language_code = str(user["language_code"] or "").strip().lower()
    if not language_code:
        return False
    lang_prefix = language_code.split("-", maxsplit=1)[0]
    return lang_prefix in ROUTE_CIS_LANG_CODES


def effective_user_route_mode(user: sqlite3.Row | None, telegram_id: int) -> str:
    selected_mode = get_user_route_mode(telegram_id)
    if selected_mode in {"main", "reserve"}:
        return selected_mode
    return "reserve" if user_is_cis_preferred(user) else "main"


def get_all_user_ids() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT telegram_id FROM users ORDER BY telegram_id").fetchall()
    conn.close()
    return [int(row["telegram_id"]) for row in rows]


def get_blacklist_reason(telegram_id: int) -> str | None:
    if int(telegram_id) in BLACKLIST_TELEGRAM_IDS:
        return "blocked_by_env"

    conn = get_conn()
    row = conn.execute(
        """
        SELECT reason
        FROM blacklisted_users
        WHERE telegram_id = ?
        LIMIT 1
        """,
        (int(telegram_id),),
    ).fetchone()
    conn.close()
    if not row:
        return None
    reason = str(row["reason"] or "").strip()
    return reason or "blocked_by_admin"


def is_user_blacklisted(telegram_id: int) -> bool:
    return get_blacklist_reason(telegram_id) is not None


def blacklist_add_user(telegram_id: int, reason: str = "", created_by: int | None = None) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO blacklisted_users (telegram_id, reason, created_at, created_by)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            reason = excluded.reason,
            created_by = excluded.created_by
        """,
        (
            int(telegram_id),
            str(reason or "").strip()[:300],
            now_str(),
            created_by,
        ),
    )
    conn.commit()
    conn.close()


def blacklist_remove_user(telegram_id: int) -> int:
    conn = get_conn()
    cursor = conn.execute(
        "DELETE FROM blacklisted_users WHERE telegram_id = ?",
        (int(telegram_id),),
    )
    deleted = int(cursor.rowcount or 0)
    conn.commit()
    conn.close()
    return deleted


def blacklist_list(limit: int = 100) -> list[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT telegram_id, reason, created_at, created_by
        FROM blacklisted_users
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, min(limit, 500)),),
    ).fetchall()
    conn.close()
    return rows


def log_suspicious_flag(
    telegram_id: int,
    flag_type: str,
    details: str = "",
    *,
    dedup_seconds: int | None = None,
) -> None:
    if not ANTIABUSE_FLAGS_ENABLED:
        return
    if int(telegram_id) <= 0:
        return

    normalized_type = str(flag_type or "").strip().lower()[:64]
    if not normalized_type:
        return

    normalized_details = str(details or "").strip()[:1000]
    now_at = dt.datetime.now()
    now_s = now_at.strftime("%Y-%m-%d %H:%M:%S")
    dedup_window = max(1, int(dedup_seconds or ANTIABUSE_FLAG_DEDUP_SECONDS))

    conn = get_conn()
    row = conn.execute(
        """
        SELECT id, last_seen_at
        FROM antiabuse_flags
        WHERE telegram_id = ?
          AND flag_type = ?
          AND COALESCE(details, '') = ?
          AND COALESCE(resolved, 0) = 0
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(telegram_id), normalized_type, normalized_details),
    ).fetchone()

    if row:
        last_seen_at = parse_date(str(row["last_seen_at"] or ""))
        if last_seen_at and (now_at - last_seen_at).total_seconds() <= dedup_window:
            conn.execute(
                """
                UPDATE antiabuse_flags
                SET last_seen_at = ?,
                    seen_count = COALESCE(seen_count, 1) + 1
                WHERE id = ?
                """,
                (now_s, int(row["id"])),
            )
            conn.commit()
            conn.close()
            return

    conn.execute(
        """
        INSERT INTO antiabuse_flags (
            telegram_id,
            flag_type,
            details,
            created_at,
            last_seen_at,
            seen_count,
            resolved
        )
        VALUES (?, ?, ?, ?, ?, 1, 0)
        """,
        (
            int(telegram_id),
            normalized_type,
            normalized_details,
            now_s,
            now_s,
        ),
    )
    conn.commit()
    conn.close()


def get_recent_suspicious_flags(limit: int = 50, unresolved_only: bool = True) -> list[sqlite3.Row]:
    conn = get_conn()
    where_clause = "WHERE COALESCE(resolved, 0) = 0" if unresolved_only else ""
    rows = conn.execute(
        f"""
        SELECT
            id,
            telegram_id,
            flag_type,
            details,
            created_at,
            last_seen_at,
            seen_count,
            resolved,
            resolved_at,
            resolved_by
        FROM antiabuse_flags
        {where_clause}
        ORDER BY last_seen_at DESC, id DESC
        LIMIT ?
        """,
        (max(1, min(int(limit), 500)),),
    ).fetchall()
    conn.close()
    return rows


def resolve_suspicious_flag(flag_id: int, resolved_by: int | None = None) -> bool:
    conn = get_conn()
    cursor = conn.execute(
        """
        UPDATE antiabuse_flags
        SET resolved = 1,
            resolved_at = ?,
            resolved_by = ?
        WHERE id = ?
          AND COALESCE(resolved, 0) = 0
        """,
        (now_str(), resolved_by, int(flag_id)),
    )
    changed = int(cursor.rowcount or 0)
    conn.commit()
    conn.close()
    return changed > 0


def prune_old_suspicious_flags(retention_days: int = 30) -> int:
    days = max(1, int(retention_days))
    cutoff = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    cursor = conn.execute(
        """
        DELETE FROM antiabuse_flags
        WHERE last_seen_at < ?
          AND COALESCE(resolved, 0) = 1
        """,
        (cutoff,),
    )
    deleted = int(cursor.rowcount or 0)
    conn.commit()
    conn.close()
    return deleted


def order_burst_retry_after_seconds(telegram_id: int) -> int:
    if ORDER_BURST_WINDOW_SECONDS <= 0 or ORDER_BURST_MAX <= 0:
        return 0

    now_at = dt.datetime.now()
    since = (now_at - dt.timedelta(seconds=ORDER_BURST_WINDOW_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt, MIN(created_at) AS oldest_at
        FROM payments
        WHERE telegram_id = ? AND created_at >= ?
        """,
        (int(telegram_id), since),
    ).fetchone()
    conn.close()

    if not row:
        return 0
    count = int(row["cnt"] or 0)
    if count < ORDER_BURST_MAX:
        return 0
    oldest_at = parse_date(str(row["oldest_at"] or ""))
    if not oldest_at:
        return ORDER_BURST_WINDOW_SECONDS
    elapsed = int((now_at - oldest_at).total_seconds())
    return max(1, ORDER_BURST_WINDOW_SECONDS - elapsed)


def order_create_retry_after_seconds(telegram_id: int) -> int:
    return order_create_retry_state(telegram_id)[0]


def order_create_retry_state(telegram_id: int) -> tuple[int, str]:
    cooldown_left = payment_order_cooldown_left(telegram_id)
    if cooldown_left > 0:
        return cooldown_left, "cooldown"
    burst_left = order_burst_retry_after_seconds(telegram_id)
    if burst_left > 0:
        return burst_left, "burst"
    return 0, "ok"


def trial_request_meta_key(telegram_id: int) -> str:
    return f"trial_request_last:{int(telegram_id)}"


def trial_request_cooldown_left(telegram_id: int) -> int:
    if TRIAL_REQUEST_COOLDOWN_SECONDS <= 0:
        return 0
    raw = get_app_meta(trial_request_meta_key(telegram_id))
    last_at = parse_date(raw)
    if not last_at:
        return 0
    elapsed = int((dt.datetime.now() - last_at).total_seconds())
    return max(0, TRIAL_REQUEST_COOLDOWN_SECONDS - elapsed)


def mark_trial_request_seen(telegram_id: int) -> None:
    set_app_meta(trial_request_meta_key(telegram_id), now_str())


def has_trial_claim_by_username(username: str | None) -> bool:
    normalized = str(username or "").strip().lstrip("@").lower()
    if not normalized:
        return False
    conn = get_conn()
    row = conn.execute(
        """
        SELECT 1
        FROM trial_claims
        WHERE LOWER(TRIM(COALESCE(username, ''))) = ?
        LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    conn.close()
    return bool(row)


def get_users_with_expiring_subscription(
    within_hours: int,
    limit: int = 500,
) -> list[sqlite3.Row]:
    window_hours = max(1, int(within_hours))
    now_at = dt.datetime.now()
    until_at = now_at + dt.timedelta(hours=window_hours)
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT telegram_id, username, subscription_end
        FROM users
        WHERE subscription_end IS NOT NULL
          AND subscription_end != ''
          AND subscription_end > ?
          AND subscription_end <= ?
        ORDER BY subscription_end ASC
        LIMIT ?
        """,
        (
            now_at.strftime("%Y-%m-%d %H:%M:%S"),
            until_at.strftime("%Y-%m-%d %H:%M:%S"),
            max(1, int(limit)),
        ),
    ).fetchall()
    conn.close()
    return rows


def get_app_meta(key: str) -> str | None:
    conn = get_conn()
    row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
    conn.close()
    if not row:
        return None
    return str(row["value"])


def set_app_meta(key: str, value: str) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO app_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def _meta_to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled", "enable"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled", "disable"}:
        return False
    return default


def get_update_notify_mode() -> str:
    value = str(get_app_meta("update_notify_mode") or "").strip().lower()
    if value in {"auto", "manual"}:
        return value
    return "auto" if UPDATE_NOTIFY_ON_START else "manual"


def update_notify_manual_mode_enabled() -> bool:
    return get_update_notify_mode() == "manual"


def set_update_notify_manual_mode(enabled: bool) -> None:
    set_app_meta("update_notify_mode", "manual" if enabled else "auto")


def maintenance_mode_enabled() -> bool:
    return _meta_to_bool(get_app_meta("maintenance_mode"), default=False)


def set_maintenance_mode(enabled: bool) -> None:
    set_app_meta("maintenance_mode", "1" if enabled else "0")


def maintenance_user_block_text() -> str:
    return (
        "🚧 Сейчас ведутся технические работы.\n"
        "Выдача и перевыпуск ключей временно недоступны.\n"
        "О завершении работ мы уведомим отдельным сообщением."
    )


def maintenance_broadcast_text(enabled: bool) -> str:
    if enabled:
        return (
            "🚧 Начались технические работы.\n"
            "Временнo недоступна выдача и перевыпуск ключей в боте.\n"
            "После завершения работ пришлем отдельное уведомление."
        )
    return (
        "✅ Закончились технические работы.\n"
        "Выдача и перевыпуск ключей снова доступны."
    )


async def send_update_notice_broadcast() -> tuple[int, int]:
    text = UPDATE_NOTIFY_TEXT or "🆕 Вышло новое обновление. Пропишите /start"
    sent, failed = await broadcast_text(text)
    set_app_meta("update_notice_version", resolve_app_version())
    return sent, failed


def _stats_window_bounds(window_hours: int) -> tuple[str, str, dt.datetime, dt.datetime]:
    end_at = dt.datetime.now()
    start_at = end_at - dt.timedelta(hours=max(1, int(window_hours)))
    return (
        start_at.strftime("%Y-%m-%d %H:%M:%S"),
        end_at.strftime("%Y-%m-%d %H:%M:%S"),
        start_at,
        end_at,
    )


def collect_revenue_sources_stats(window_hours: int | None = None) -> dict[str, dict[str, float | int]]:
    conn = get_conn()
    referred_rows = conn.execute("SELECT invited_telegram_id FROM referrals").fetchall()
    referred_ids = {int(row["invited_telegram_id"]) for row in referred_rows if row["invited_telegram_id"] is not None}

    params: list[Any] = []
    where_parts = ["status = 'paid'", "paid_at IS NOT NULL"]
    if window_hours is not None:
        start_s, end_s, _, _ = _stats_window_bounds(window_hours)
        where_parts.append("paid_at >= ?")
        where_parts.append("paid_at <= ?")
        params.extend([start_s, end_s])

    where_sql = " AND ".join(where_parts)
    rows = conn.execute(
        f"""
        SELECT telegram_id, amount_rub, promo_code, promo_discount_rub
        FROM payments
        WHERE {where_sql}
        """,
        params,
    ).fetchall()
    conn.close()

    stats: dict[str, dict[str, float | int]] = {
        "organic": {"count": 0, "revenue_rub": 0.0},
        "referral": {"count": 0, "revenue_rub": 0.0},
        "promo": {"count": 0, "revenue_rub": 0.0},
    }

    for row in rows:
        telegram_id = int(row["telegram_id"] or 0)
        amount_rub = float(row["amount_rub"] or 0.0)
        promo_code = str(row["promo_code"] or "").strip()
        promo_discount = int(row["promo_discount_rub"] or 0)
        is_promo = bool(promo_code and promo_discount > 0)
        is_referral = telegram_id in referred_ids

        source = "organic"
        if is_promo:
            source = "promo"
        elif is_referral:
            source = "referral"

        stats[source]["count"] = int(stats[source]["count"]) + 1
        stats[source]["revenue_rub"] = float(stats[source]["revenue_rub"]) + amount_rub

    return stats


def format_revenue_sources_lines(stats: dict[str, dict[str, float | int]]) -> str:
    def line(label: str, key: str) -> str:
        item = stats.get(key, {})
        count = int(item.get("count", 0))
        revenue = format_rub_value(float(item.get("revenue_rub", 0.0)))
        return f"• {label}: {count} оплат, {revenue} ₽"

    return "\n".join(
        [
            line("Organic", "organic"),
            line("Referral", "referral"),
            line("Promo", "promo"),
        ]
    )


def collect_admin_window_stats(window_hours: int = 24) -> dict[str, Any]:
    start_s, end_s, start_at, end_at = _stats_window_bounds(window_hours)
    conn = get_conn()
    paid_row = conn.execute(
        """
        SELECT
            COUNT(*) AS paid_count,
            COALESCE(SUM(amount_rub), 0) AS revenue_rub
        FROM payments
        WHERE status = 'paid'
          AND paid_at IS NOT NULL
          AND paid_at >= ?
          AND paid_at <= ?
        """,
        (start_s, end_s),
    ).fetchone()
    new_users_row = conn.execute(
        """
        SELECT COUNT(*) AS new_users
        FROM users
        WHERE created_at IS NOT NULL
          AND created_at != ''
          AND created_at >= ?
          AND created_at <= ?
        """,
        (start_s, end_s),
    ).fetchone()
    trials_row = conn.execute(
        """
        SELECT COUNT(*) AS trial_claims
        FROM trial_claims
        WHERE claimed_at >= ?
          AND claimed_at <= ?
        """,
        (start_s, end_s),
    ).fetchone()
    trial_paid_row = conn.execute(
        """
        SELECT COUNT(*) AS paid_from_trials
        FROM trial_claims t
        WHERE t.claimed_at >= ?
          AND t.claimed_at <= ?
          AND EXISTS (
              SELECT 1
              FROM payments p
              WHERE p.telegram_id = t.telegram_id
                AND p.status = 'paid'
          )
        """,
        (start_s, end_s),
    ).fetchone()
    paid_users_row = conn.execute(
        """
        SELECT COUNT(DISTINCT telegram_id) AS paid_users
        FROM payments
        WHERE status = 'paid'
          AND paid_at IS NOT NULL
          AND paid_at >= ?
          AND paid_at <= ?
        """,
        (start_s, end_s),
    ).fetchone()
    repeat_buyers_row = conn.execute(
        """
        SELECT COUNT(DISTINCT p.telegram_id) AS cnt
        FROM payments p
        WHERE p.status = 'paid'
          AND p.paid_at IS NOT NULL
          AND p.paid_at >= ?
          AND p.paid_at <= ?
          AND EXISTS (
              SELECT 1
              FROM payments p2
              WHERE p2.telegram_id = p.telegram_id
                AND p2.status = 'paid'
                AND COALESCE(p2.paid_at, p2.created_at) < ?
          )
        """,
        (start_s, end_s, start_s),
    ).fetchone()
    promo_row = conn.execute(
        """
        SELECT
            COUNT(*) AS promo_count,
            COALESCE(SUM(amount_rub), 0) AS promo_revenue_rub,
            COALESCE(SUM(promo_discount_rub), 0) AS promo_discount_rub
        FROM payments
        WHERE status = 'paid'
          AND paid_at IS NOT NULL
          AND paid_at >= ?
          AND paid_at <= ?
          AND promo_code IS NOT NULL
          AND promo_code != ''
          AND promo_discount_rub > 0
        """,
        (start_s, end_s),
    ).fetchone()
    conn.close()

    trial_claims = int(trials_row["trial_claims"] or 0) if trials_row else 0
    paid_from_trials = int(trial_paid_row["paid_from_trials"] or 0) if trial_paid_row else 0
    conversion = (paid_from_trials * 100.0 / trial_claims) if trial_claims > 0 else 0.0
    revenue = float(paid_row["revenue_rub"] or 0) if paid_row else 0.0
    paid_count = int(paid_row["paid_count"] or 0) if paid_row else 0
    paid_users = int(paid_users_row["paid_users"] or 0) if paid_users_row else 0
    repeat_buyers = int(repeat_buyers_row["cnt"] or 0) if repeat_buyers_row else 0
    avg_check_rub = revenue / paid_count if paid_count > 0 else 0.0
    arppu_rub = revenue / paid_users if paid_users > 0 else 0.0
    repeat_rate_percent = (repeat_buyers * 100.0 / paid_users) if paid_users > 0 else 0.0
    promo_count = int(promo_row["promo_count"] or 0) if promo_row else 0
    promo_revenue_rub = float(promo_row["promo_revenue_rub"] or 0) if promo_row else 0.0
    promo_discount_rub = float(promo_row["promo_discount_rub"] or 0) if promo_row else 0.0
    promo_share_percent = (promo_count * 100.0 / paid_count) if paid_count > 0 else 0.0
    new_users = int(new_users_row["new_users"] or 0) if new_users_row else 0
    source_stats = collect_revenue_sources_stats(window_hours)

    return {
        "window_hours": max(1, int(window_hours)),
        "from": start_at,
        "to": end_at,
        "paid_count": paid_count,
        "paid_users": paid_users,
        "avg_check_rub": avg_check_rub,
        "arppu_rub": arppu_rub,
        "repeat_buyers": repeat_buyers,
        "repeat_rate_percent": repeat_rate_percent,
        "promo_count": promo_count,
        "promo_revenue_rub": promo_revenue_rub,
        "promo_discount_rub": promo_discount_rub,
        "promo_share_percent": promo_share_percent,
        "revenue_rub": revenue,
        "new_users": new_users,
        "trial_claims": trial_claims,
        "paid_from_trials": paid_from_trials,
        "trial_conversion_percent": conversion,
        "sources": source_stats,
    }


def collect_admin_totals_stats() -> dict[str, Any]:
    now_s = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    users_row = conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
    active_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM users
        WHERE subscription_end IS NOT NULL
          AND subscription_end != ''
          AND subscription_end > ?
        """,
        (now_s,),
    ).fetchone()
    paid_row = conn.execute(
        """
        SELECT COUNT(*) AS paid_count, COALESCE(SUM(amount_rub), 0) AS revenue_rub
        FROM payments
        WHERE status = 'paid'
        """,
    ).fetchone()
    paid_users_row = conn.execute(
        """
        SELECT COUNT(DISTINCT telegram_id) AS cnt
        FROM payments
        WHERE status = 'paid'
        """,
    ).fetchone()
    repeat_buyers_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT telegram_id
            FROM payments
            WHERE status = 'paid'
            GROUP BY telegram_id
            HAVING COUNT(*) >= 2
        ) t
        """,
    ).fetchone()
    churn_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT p.telegram_id, MAX(COALESCE(u.subscription_end, '')) AS subscription_end
            FROM payments p
            LEFT JOIN users u ON u.telegram_id = p.telegram_id
            WHERE p.status = 'paid'
            GROUP BY p.telegram_id
        ) t
        WHERE subscription_end = '' OR subscription_end <= ?
        """,
        (now_s,),
    ).fetchone()
    promo_row = conn.execute(
        """
        SELECT
            COUNT(*) AS promo_count,
            COALESCE(SUM(amount_rub), 0) AS promo_revenue_rub,
            COALESCE(SUM(promo_discount_rub), 0) AS promo_discount_rub
        FROM payments
        WHERE status = 'paid'
          AND promo_code IS NOT NULL
          AND promo_code != ''
          AND promo_discount_rub > 0
        """,
    ).fetchone()
    trial_row = conn.execute("SELECT COUNT(*) AS trial_claims FROM trial_claims").fetchone()
    trial_paid_row = conn.execute(
        """
        SELECT COUNT(*) AS paid_from_trials
        FROM trial_claims t
        WHERE EXISTS (
            SELECT 1
            FROM payments p
            WHERE p.telegram_id = t.telegram_id
              AND p.status = 'paid'
        )
        """,
    ).fetchone()
    conn.close()

    trial_claims = int(trial_row["trial_claims"] or 0) if trial_row else 0
    paid_from_trials = int(trial_paid_row["paid_from_trials"] or 0) if trial_paid_row else 0
    conversion = (paid_from_trials * 100.0 / trial_claims) if trial_claims > 0 else 0.0
    users_total = int(users_row["cnt"] or 0) if users_row else 0
    paid_count_total = int(paid_row["paid_count"] or 0) if paid_row else 0
    revenue_total_rub = float(paid_row["revenue_rub"] or 0) if paid_row else 0.0
    paid_users_total = int(paid_users_row["cnt"] or 0) if paid_users_row else 0
    repeat_buyers_total = int(repeat_buyers_row["cnt"] or 0) if repeat_buyers_row else 0
    churn_paid_users_total = int(churn_row["cnt"] or 0) if churn_row else 0
    promo_count_total = int(promo_row["promo_count"] or 0) if promo_row else 0
    promo_revenue_total_rub = float(promo_row["promo_revenue_rub"] or 0) if promo_row else 0.0
    promo_discount_total_rub = float(promo_row["promo_discount_rub"] or 0) if promo_row else 0.0
    avg_check_total_rub = revenue_total_rub / paid_count_total if paid_count_total > 0 else 0.0
    arppu_total_rub = revenue_total_rub / paid_users_total if paid_users_total > 0 else 0.0
    ltv_total_rub = revenue_total_rub / users_total if users_total > 0 else 0.0
    repeat_rate_total_percent = (
        repeat_buyers_total * 100.0 / paid_users_total
    ) if paid_users_total > 0 else 0.0
    churn_total_percent = (
        churn_paid_users_total * 100.0 / paid_users_total
    ) if paid_users_total > 0 else 0.0
    promo_share_total_percent = (
        promo_count_total * 100.0 / paid_count_total
    ) if paid_count_total > 0 else 0.0
    source_stats = collect_revenue_sources_stats(window_hours=None)
    return {
        "users_total": users_total,
        "active_subscriptions": int(active_row["cnt"] or 0) if active_row else 0,
        "paid_count_total": paid_count_total,
        "paid_users_total": paid_users_total,
        "revenue_total_rub": revenue_total_rub,
        "avg_check_total_rub": avg_check_total_rub,
        "arppu_total_rub": arppu_total_rub,
        "ltv_total_rub": ltv_total_rub,
        "repeat_buyers_total": repeat_buyers_total,
        "repeat_rate_total_percent": repeat_rate_total_percent,
        "churn_paid_users_total": churn_paid_users_total,
        "churn_total_percent": churn_total_percent,
        "promo_count_total": promo_count_total,
        "promo_revenue_total_rub": promo_revenue_total_rub,
        "promo_discount_total_rub": promo_discount_total_rub,
        "promo_share_total_percent": promo_share_total_percent,
        "trial_claims_total": trial_claims,
        "paid_from_trials_total": paid_from_trials,
        "trial_conversion_total_percent": conversion,
        "sources": source_stats,
    }


def format_admin_stats_text() -> str:
    daily = collect_admin_window_stats(24)
    weekly = collect_admin_window_stats(24 * 7)
    totals = collect_admin_totals_stats()
    return (
        "📊 Статистика BoxVolt\n\n"
        "За 24 часа:\n"
        f"• Выручка: {format_rub_value(daily['revenue_rub'])} ₽\n"
        f"• Успешных оплат: {daily['paid_count']}\n"
        f"• Платящих пользователей: {daily['paid_users']}\n"
        f"• Средний чек: {format_rub_value(daily['avg_check_rub'])} ₽\n"
        f"• ARPPU: {format_rub_value(daily['arppu_rub'])} ₽\n"
        f"• Повторные покупатели: {daily['repeat_buyers']} ({daily['repeat_rate_percent']:.1f}%)\n"
        f"• Promo-оплат: {daily['promo_count']} ({daily['promo_share_percent']:.1f}%), "
        f"скидок: {format_rub_value(daily['promo_discount_rub'])} ₽\n"
        f"• Новых пользователей: {daily['new_users']}\n"
        f"• Взяли тест: {daily['trial_claims']}\n"
        f"• Конверсия теста: {daily['trial_conversion_percent']:.1f}% "
        f"({daily['paid_from_trials']}/{daily['trial_claims'] or 0})\n"
        "• Источники:\n"
        f"{format_revenue_sources_lines(daily['sources'])}\n\n"
        "За 7 дней:\n"
        f"• Выручка: {format_rub_value(weekly['revenue_rub'])} ₽\n"
        f"• Успешных оплат: {weekly['paid_count']}\n"
        f"• Платящих пользователей: {weekly['paid_users']}\n"
        f"• ARPPU: {format_rub_value(weekly['arppu_rub'])} ₽\n"
        f"• Повторные покупатели: {weekly['repeat_buyers']} ({weekly['repeat_rate_percent']:.1f}%)\n"
        f"• Новых пользователей: {weekly['new_users']}\n"
        f"• Конверсия теста: {weekly['trial_conversion_percent']:.1f}% "
        f"({weekly['paid_from_trials']}/{weekly['trial_claims'] or 0})\n\n"
        "Всего:\n"
        f"• Пользователей: {totals['users_total']}\n"
        f"• Активных подписок: {totals['active_subscriptions']}\n"
        f"• Выручка: {format_rub_value(totals['revenue_total_rub'])} ₽\n"
        f"• Оплат: {totals['paid_count_total']}\n"
        f"• Платящих пользователей: {totals['paid_users_total']}\n"
        f"• LTV: {format_rub_value(totals['ltv_total_rub'])} ₽\n"
        f"• ARPPU: {format_rub_value(totals['arppu_total_rub'])} ₽\n"
        f"• Повторные покупатели: {totals['repeat_buyers_total']} ({totals['repeat_rate_total_percent']:.1f}%)\n"
        f"• Churn (snapshot): {totals['churn_total_percent']:.1f}% ({totals['churn_paid_users_total']}/{totals['paid_users_total'] or 0})\n"
        f"• Promo-оплат: {totals['promo_count_total']} ({totals['promo_share_total_percent']:.1f}%), "
        f"скидок: {format_rub_value(totals['promo_discount_total_rub'])} ₽\n"
        f"• Конверсия теста (общая): {totals['trial_conversion_total_percent']:.1f}% "
        f"({totals['paid_from_trials_total']}/{totals['trial_claims_total'] or 0})\n"
        "• Источники (всего):\n"
        f"{format_revenue_sources_lines(totals['sources'])}"
    )


def get_admin_notification_targets() -> list[int]:
    if ADMIN_NOTIFY_CHAT_IDS:
        return sorted(int(x) for x in ADMIN_NOTIFY_CHAT_IDS)
    return sorted(int(x) for x in ADMIN_TELEGRAM_IDS)


async def send_admin_notification_text(text: str, parse_mode: str | None = None) -> tuple[int, int]:
    targets = get_admin_notification_targets()
    if not targets:
        return 0, 0

    sent = 0
    failed = 0
    for target_chat_id in targets:
        send_kwargs: dict[str, Any] = {}
        if parse_mode:
            send_kwargs["parse_mode"] = parse_mode
        use_topic = ADMIN_NOTIFY_TOPIC_ID > 0 and int(target_chat_id) < 0
        if use_topic:
            send_kwargs["message_thread_id"] = ADMIN_NOTIFY_TOPIC_ID
        try:
            await bot.send_message(int(target_chat_id), text, **send_kwargs)
            sent += 1
            continue
        except Exception:
            if use_topic:
                try:
                    fallback_kwargs = dict(send_kwargs)
                    fallback_kwargs.pop("message_thread_id", None)
                    await bot.send_message(int(target_chat_id), text, **fallback_kwargs)
                    sent += 1
                    print(
                        f"[admin-notify] Topic send failed for {target_chat_id}, fallback to main chat"
                    )
                    continue
                except Exception:
                    pass
            failed += 1
    return sent, failed


def admin_daily_report_meta_key() -> str:
    return "admin_daily_report_last_at"


async def maybe_send_admin_daily_report(force: bool = False) -> tuple[bool, str]:
    if not ADMIN_DAILY_REPORT_ENABLED and not force:
        return False, "disabled"
    if not get_admin_notification_targets():
        return False, "no_targets"

    now_at = dt.datetime.now()
    if not force:
        last_s = get_app_meta(admin_daily_report_meta_key())
        last_at = parse_date(last_s)
        if last_at and (now_at - last_at).total_seconds() < ADMIN_DAILY_REPORT_INTERVAL_SECONDS:
            return False, "too_early"

    daily = collect_admin_window_stats(ADMIN_DAILY_REPORT_WINDOW_HOURS)
    text = (
        "📈 Ежедневный отчет BoxVolt\n"
        f"Период: последние {daily['window_hours']} ч.\n\n"
        f"• Выручка: {format_rub_value(daily['revenue_rub'])} ₽\n"
        f"• Успешных оплат: {daily['paid_count']}\n"
        f"• Платящих пользователей: {daily['paid_users']}\n"
        f"• Средний чек: {format_rub_value(daily['avg_check_rub'])} ₽\n"
        f"• ARPPU: {format_rub_value(daily['arppu_rub'])} ₽\n"
        f"• Повторные покупатели: {daily['repeat_buyers']} ({daily['repeat_rate_percent']:.1f}%)\n"
        f"• Promo-оплат: {daily['promo_count']} ({daily['promo_share_percent']:.1f}%), "
        f"скидок: {format_rub_value(daily['promo_discount_rub'])} ₽\n"
        f"• Новых пользователей: {daily['new_users']}\n"
        f"• Взяли тест: {daily['trial_claims']}\n"
        f"• Конверсия теста: {daily['trial_conversion_percent']:.1f}% "
        f"({daily['paid_from_trials']}/{daily['trial_claims'] or 0})\n"
        "• Источники:\n"
        f"{format_revenue_sources_lines(daily['sources'])}\n"
    )

    sent, failed = await send_admin_notification_text(text)
    if sent > 0:
        set_app_meta(admin_daily_report_meta_key(), now_str())
        return True, f"sent={sent} failed={failed}"
    return False, f"sent={sent} failed={failed}"


async def admin_daily_report_loop() -> None:
    interval = max(60, ADMIN_DAILY_REPORT_CHECK_INTERVAL_SECONDS)
    while True:
        try:
            sent, reason = await maybe_send_admin_daily_report(force=False)
            if sent:
                print(f"[admin-report] Daily report {reason}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[admin-report] Loop error: {exc}")

        await asyncio.sleep(interval)


def resolve_app_version() -> str:
    global APP_RUNTIME_VERSION_CACHE
    if APP_RUNTIME_VERSION_CACHE:
        return APP_RUNTIME_VERSION_CACHE

    if APP_VERSION:
        APP_RUNTIME_VERSION_CACHE = APP_VERSION
        return APP_RUNTIME_VERSION_CACHE

    # Auto-version fallback: tied to current bot.py contents when APP_VERSION is empty.
    try:
        digest = hashlib.sha1(Path(__file__).read_bytes()).hexdigest()[:12]
        APP_RUNTIME_VERSION_CACHE = f"auto-{digest}"
    except OSError:
        APP_RUNTIME_VERSION_CACHE = ""
    return APP_RUNTIME_VERSION_CACHE


async def maybe_send_update_notification() -> None:
    if get_update_notify_mode() != "auto":
        return

    current_version = resolve_app_version()
    if not current_version:
        return

    last_version = get_app_meta("update_notice_version")
    if last_version == current_version:
        return

    sent, failed = await send_update_notice_broadcast()
    print(
        f"[update-notify] version={current_version} sent={sent} failed={failed}"
    )


def user_has_paid_payment(telegram_id: int) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM payments WHERE telegram_id = ? AND status = 'paid' LIMIT 1",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return bool(row)


def normalize_promo_code(code: str) -> str:
    return str(code or "").strip().upper()


def parse_datetime_input(value: str) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")
    for fmt in formats:
        try:
            parsed = dt.datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                parsed = parsed.replace(hour=23, minute=59, second=59)
            elif fmt == "%Y-%m-%d %H:%M":
                parsed = parsed.replace(second=59)
            return parsed
        except ValueError:
            continue
    return None


def clamp_discount_to_amount(amount_rub: int, discount_rub: int) -> int:
    if amount_rub <= 1:
        return 0
    return max(0, min(discount_rub, amount_rub - 1))


def create_or_update_promocode(
    code: str,
    discount_rub: int,
    expires_at: str,
    max_activations: int,
    created_by: int | None = None,
) -> tuple[bool, str]:
    normalized_code = normalize_promo_code(code)
    if not PROMO_CODE_RE.fullmatch(normalized_code):
        return False, "bad_code_format"

    parsed_expiry = parse_datetime_input(expires_at)
    if not parsed_expiry:
        return False, "bad_expiry_format"
    if parsed_expiry <= dt.datetime.now():
        return False, "expiry_in_past"

    discount = _clamp(_safe_int(discount_rub, 0), 1, 100000)
    max_uses = _clamp(_safe_int(max_activations, 0), 0, 1_000_000)

    conn = get_conn()
    conn.execute(
        """
        INSERT INTO promo_codes (
            code, discount_rub, expires_at, max_activations, is_active, created_at, created_by
        )
        VALUES (?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            discount_rub = excluded.discount_rub,
            expires_at = excluded.expires_at,
            max_activations = excluded.max_activations,
            is_active = 1,
            created_by = excluded.created_by
        """,
        (
            normalized_code,
            discount,
            parsed_expiry.strftime("%Y-%m-%d %H:%M:%S"),
            max_uses,
            now_str(),
            created_by,
        ),
    )
    conn.commit()
    conn.close()
    return True, "ok"


def get_promocode_activation_stats(code: str) -> tuple[int, int]:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_activations,
            COALESCE(SUM(CASE WHEN used_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS used_activations
        FROM promo_activations
        WHERE code = ?
        """,
        (code,),
    ).fetchone()
    conn.close()
    if not row:
        return 0, 0
    return int(row["total_activations"] or 0), int(row["used_activations"] or 0)


def get_promocodes_for_admin(limit: int = 50) -> list[dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT code, discount_rub, expires_at, max_activations, is_active, created_at, created_by
        FROM promo_codes
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, min(limit, 200)),),
    ).fetchall()
    conn.close()

    result: list[dict[str, Any]] = []
    now = dt.datetime.now()
    for row in rows:
        total, used = get_promocode_activation_stats(str(row["code"]))
        expires = parse_date(row["expires_at"])
        result.append(
            {
                "code": row["code"],
                "discount_rub": int(row["discount_rub"] or 0),
                "expires_at": row["expires_at"],
                "max_activations": int(row["max_activations"] or 0),
                "is_active": int(row["is_active"] or 0) == 1,
                "created_at": row["created_at"],
                "created_by": row["created_by"],
                "total_activations": total,
                "used_activations": used,
                "is_expired": bool(expires and expires <= now),
            }
        )
    return result


def get_user_active_promocode(telegram_id: int) -> dict[str, Any] | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            a.id,
            a.code,
            a.activated_at,
            c.discount_rub,
            c.expires_at,
            c.max_activations
        FROM promo_activations a
        INNER JOIN promo_codes c ON c.code = a.code
        WHERE a.telegram_id = ?
          AND a.used_at IS NULL
          AND c.is_active = 1
          AND c.expires_at > ?
        ORDER BY a.activated_at DESC
        LIMIT 1
        """,
        (telegram_id, now_str()),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "activation_id": int(row["id"]),
        "code": str(row["code"]),
        "discount_rub": int(row["discount_rub"] or 0),
        "expires_at": row["expires_at"],
        "activated_at": row["activated_at"],
        "max_activations": int(row["max_activations"] or 0),
    }


def activate_promocode_for_user(telegram_id: int, code: str) -> tuple[bool, str, dict[str, Any] | None]:
    normalized_code = normalize_promo_code(code)
    if not PROMO_CODE_RE.fullmatch(normalized_code):
        return False, "bad_code_format", None

    conn = get_conn()
    promo = conn.execute(
        """
        SELECT code, discount_rub, expires_at, max_activations, is_active
        FROM promo_codes
        WHERE code = ?
        """,
        (normalized_code,),
    ).fetchone()
    if not promo:
        conn.close()
        return False, "promo_not_found", None

    if int(promo["is_active"] or 0) != 1:
        conn.close()
        return False, "promo_inactive", None

    expires = parse_date(promo["expires_at"])
    if not expires or expires <= dt.datetime.now():
        conn.close()
        return False, "promo_expired", None

    max_activations = int(promo["max_activations"] or 0)
    if max_activations > 0:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM promo_activations WHERE code = ?",
            (normalized_code,),
        ).fetchone()
        if int(row["cnt"] or 0) >= max_activations:
            conn.close()
            return False, "promo_limit_reached", None

    try:
        conn.execute(
            """
            INSERT INTO promo_activations (code, telegram_id, activated_at)
            VALUES (?, ?, ?)
            """,
            (normalized_code, telegram_id, now_str()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return False, "promo_already_activated", None

    conn.close()
    active = get_user_active_promocode(telegram_id)
    return True, "ok", active


def promo_error_text(reason: str) -> str:
    mapping = {
        "bad_code_format": "Неверный формат промокода.",
        "promo_not_found": "Промокод не найден.",
        "promo_inactive": "Промокод отключен.",
        "promo_expired": "Срок действия промокода истек.",
        "promo_limit_reached": "Лимит активаций промокода исчерпан.",
        "promo_already_activated": "Вы уже активировали этот промокод.",
    }
    return mapping.get(reason, "Не удалось активировать промокод.")


def has_trial_claim(telegram_id: int) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM trial_claims WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return bool(row)


def mark_trial_claim(telegram_id: int, username: str | None) -> bool:
    conn = get_conn()
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO trial_claims (telegram_id, username, claimed_at)
        VALUES (?, ?, ?)
        """,
        (telegram_id, username, now_str()),
    )
    inserted = int(cursor.rowcount or 0) > 0
    conn.commit()
    conn.close()
    return inserted


def parse_referrer_id_from_start_arg(raw_arg: str) -> int | None:
    raw = str(raw_arg or "").strip()
    if not raw:
        return None
    token = raw.split(maxsplit=1)[0].strip()
    if not token:
        return None
    match = REF_START_RE.fullmatch(token)
    if not match:
        return None
    try:
        referrer_id = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return referrer_id if referrer_id > 0 else None


def normalize_referral_code(raw_code: str | None) -> str:
    return str(raw_code or "").strip().upper()


def is_valid_referral_code(code: str) -> bool:
    normalized = normalize_referral_code(code)
    return bool(re.fullmatch(rf"[A-Z0-9]{{{REFERRAL_LINK_CODE_LENGTH}}}", normalized))


def generate_referral_code() -> str:
    while True:
        code = uuid.uuid4().hex.upper()[:REFERRAL_LINK_CODE_LENGTH]
        if is_valid_referral_code(code):
            return code


def create_referral_link(
    referrer_telegram_id: int,
    *,
    label: str | None = None,
) -> str:
    clean_label = str(label or "").strip()[:REFERRAL_CAMPAIGN_LABEL_MAX] or "default"
    conn = get_conn()
    existing = conn.execute(
        """
        SELECT code
        FROM referral_links
        WHERE referrer_telegram_id = ? AND label = ?
        LIMIT 1
        """,
        (int(referrer_telegram_id), clean_label),
    ).fetchone()
    if existing:
        conn.close()
        return str(existing["code"] or "").strip().upper()

    created_code = ""
    for _ in range(8):
        candidate = generate_referral_code()
        try:
            conn.execute(
                """
                INSERT INTO referral_links (code, referrer_telegram_id, label, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (candidate, int(referrer_telegram_id), clean_label, now_str()),
            )
            conn.commit()
            created_code = candidate
            break
        except sqlite3.IntegrityError:
            continue
    conn.close()
    if created_code:
        return created_code
    raise RuntimeError("failed_to_create_referral_code")


def get_or_create_default_referral_code(referrer_telegram_id: int) -> str:
    return create_referral_link(referrer_telegram_id, label="default")


def get_referrer_id_by_referral_code(referral_code: str) -> int | None:
    code = normalize_referral_code(referral_code)
    if not is_valid_referral_code(code):
        return None
    conn = get_conn()
    row = conn.execute(
        """
        SELECT referrer_telegram_id
        FROM referral_links
        WHERE code = ?
        LIMIT 1
        """,
        (code,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    value = int(row["referrer_telegram_id"] or 0)
    return value if value > 0 else None


def parse_referral_start_payload(raw_arg: str) -> tuple[int | None, str | None]:
    raw = str(raw_arg or "").strip()
    if not raw:
        return None, None
    token = raw.split(maxsplit=1)[0].strip()
    if not token:
        return None, None

    legacy_referrer_id = parse_referrer_id_from_start_arg(token)
    if legacy_referrer_id:
        return legacy_referrer_id, None

    code = normalize_referral_code(token)
    referrer_by_code = get_referrer_id_by_referral_code(code)
    if referrer_by_code:
        return referrer_by_code, code
    return None, None


def build_referral_start_arg(referral_code: str) -> str:
    return normalize_referral_code(referral_code)


def get_referrer_id(invited_telegram_id: int) -> int | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT referrer_telegram_id
        FROM referrals
        WHERE invited_telegram_id = ?
        LIMIT 1
        """,
        (invited_telegram_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    referrer = int(row["referrer_telegram_id"] or 0)
    return referrer if referrer > 0 else None


def link_referral_if_possible(
    invited_telegram_id: int,
    invited_username: str | None,
    referrer_telegram_id: int,
    *,
    referral_code: str | None = None,
) -> tuple[bool, str]:
    if not REFERRAL_ENABLED:
        return False, "referral_disabled"
    if invited_telegram_id <= 0 or referrer_telegram_id <= 0:
        return False, "bad_telegram_id"
    if invited_telegram_id == referrer_telegram_id:
        return False, "self_referral"
    if user_has_paid_payment(invited_telegram_id):
        return False, "already_paid"

    conn = get_conn()
    existing = conn.execute(
        """
        SELECT referrer_telegram_id
        FROM referrals
        WHERE invited_telegram_id = ?
        LIMIT 1
        """,
        (invited_telegram_id,),
    ).fetchone()
    if existing:
        conn.close()
        existing_referrer = int(existing["referrer_telegram_id"] or 0)
        if existing_referrer == referrer_telegram_id:
            return False, "already_linked_same"
        return False, "already_linked_other"

    conn.execute("INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)", (referrer_telegram_id, None))
    conn.execute(
        """
        INSERT INTO referrals (invited_telegram_id, referrer_telegram_id, referral_code, invited_username, linked_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            invited_telegram_id,
            referrer_telegram_id,
            normalize_referral_code(referral_code) or None,
            invited_username,
            now_str(),
        ),
    )
    conn.commit()
    conn.close()
    return True, "linked"


def get_referral_stats(referrer_telegram_id: int) -> dict[str, int]:
    conn = get_conn()
    invited_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM referrals WHERE referrer_telegram_id = ?",
        (referrer_telegram_id,),
    ).fetchone()
    rewards_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(reward_days), 0) AS days_sum
        FROM referral_rewards
        WHERE referrer_telegram_id = ?
        """,
        (referrer_telegram_id,),
    ).fetchone()
    links_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM referral_links
        WHERE referrer_telegram_id = ?
        """,
        (referrer_telegram_id,),
    ).fetchone()
    conn.close()
    return {
        "invited_count": int(invited_row["cnt"] or 0) if invited_row else 0,
        "reward_events": int(rewards_row["cnt"] or 0) if rewards_row else 0,
        "reward_days_total": int(rewards_row["days_sum"] or 0) if rewards_row else 0,
        "links_count": int(links_row["cnt"] or 0) if links_row else 0,
    }


def get_referral_link_breakdown(referrer_telegram_id: int, limit: int = 5) -> list[dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            l.code,
            COALESCE(NULLIF(l.label, ''), 'default') AS label,
            l.created_at,
            (
                SELECT COUNT(*)
                FROM referrals r
                WHERE r.referrer_telegram_id = l.referrer_telegram_id
                  AND UPPER(COALESCE(r.referral_code, '')) = l.code
            ) AS invited_count,
            (
                SELECT COUNT(*)
                FROM referral_rewards rr
                JOIN referrals r2 ON r2.invited_telegram_id = rr.invited_telegram_id
                WHERE rr.referrer_telegram_id = l.referrer_telegram_id
                  AND UPPER(COALESCE(r2.referral_code, '')) = l.code
            ) AS paid_count
        FROM referral_links l
        WHERE l.referrer_telegram_id = ?
        ORDER BY invited_count DESC, paid_count DESC, l.created_at DESC
        LIMIT ?
        """,
        (int(referrer_telegram_id), max(1, min(int(limit), 20))),
    ).fetchall()
    conn.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "code": str(row["code"] or "").strip().upper(),
                "label": str(row["label"] or "").strip(),
                "created_at": str(row["created_at"] or ""),
                "invited_count": int(row["invited_count"] or 0),
                "paid_count": int(row["paid_count"] or 0),
            }
        )
    return result


async def get_bot_public_username() -> str:
    global BOT_PUBLIC_USERNAME_CACHE
    if BOT_PUBLIC_USERNAME_CACHE:
        return BOT_PUBLIC_USERNAME_CACHE
    try:
        me = await bot.get_me()
    except Exception:  # noqa: BLE001
        return ""
    username = str(getattr(me, "username", "") or "").strip().lstrip("@")
    if username:
        BOT_PUBLIC_USERNAME_CACHE = username
    return username


def bot_public_username_hint() -> str:
    return str(BOT_PUBLIC_USERNAME_CACHE or BOT_USERNAME or "").strip().lstrip("@")


def parse_start_order_id(start_token: str | None) -> str | None:
    raw = str(start_token or "").strip()
    if not raw:
        return None

    direct = raw.upper()
    if ORDER_ID_RE.fullmatch(direct):
        return direct

    match = START_ORDER_RE.fullmatch(raw)
    if not match:
        return None

    order_id = str(match.group(1) or "").strip().upper()
    if not ORDER_ID_RE.fullmatch(order_id):
        return None
    return order_id


def build_order_start_payload(order_id: str) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    return f"pay_{clean_order_id}" if clean_order_id else "pay"


def build_bot_start_url(payload: str = "") -> str:
    username = bot_public_username_hint()
    if not username:
        return ""

    base = f"https://t.me/{username}"
    clean_payload = str(payload or "").strip()
    if not clean_payload:
        return base
    return _append_query_params(base, {"start": clean_payload})


def build_bot_startapp_url(payload: str = "") -> str:
    username = bot_public_username_hint()
    if not username:
        return ""

    base = f"https://t.me/{username}/app"
    clean_payload = str(payload or "").strip()
    if not clean_payload:
        return base
    return _append_query_params(base, {"startapp": clean_payload})


async def build_referral_link(referrer_telegram_id: int) -> str:
    try:
        code = get_or_create_default_referral_code(referrer_telegram_id)
        payload = build_referral_start_arg(code)
    except Exception:  # noqa: BLE001
        payload = f"ref_{int(referrer_telegram_id)}"
    username = await get_bot_public_username()
    if username:
        return f"https://t.me/{username}?start={payload}"
    return f"/start {payload}"


def build_referral_share_url(referral_link: str) -> str:
    share_text = (
        "Подключайся к BoxVolt VPN. "
        "Оформляй подписку от 14 дней и получай стабильный VPN."
    )
    return _append_query_params(
        "https://t.me/share/url",
        {"url": referral_link, "text": share_text},
    )


def apply_referral_reward_for_paid_order(order_id: str) -> dict[str, Any] | None:
    if not REFERRAL_ENABLED:
        return None

    conn = get_conn()
    payment = conn.execute(
        """
        SELECT order_id, telegram_id, days, status
        FROM payments
        WHERE order_id = ?
        LIMIT 1
        """,
        (order_id,),
    ).fetchone()
    if not payment or str(payment["status"]) != "paid":
        conn.close()
        return None

    invited_telegram_id = int(payment["telegram_id"] or 0)
    paid_days = int(payment["days"] or 0)
    if invited_telegram_id <= 0 or paid_days < REFERRAL_MIN_PLAN_DAYS:
        conn.close()
        return None

    ref_row = conn.execute(
        """
        SELECT referrer_telegram_id
        FROM referrals
        WHERE invited_telegram_id = ?
        LIMIT 1
        """,
        (invited_telegram_id,),
    ).fetchone()
    if not ref_row:
        conn.close()
        return None

    referrer_telegram_id = int(ref_row["referrer_telegram_id"] or 0)
    if referrer_telegram_id <= 0 or referrer_telegram_id == invited_telegram_id:
        conn.close()
        return None

    exists = conn.execute(
        "SELECT 1 FROM referral_rewards WHERE order_id = ? LIMIT 1",
        (order_id,),
    ).fetchone()
    if exists:
        conn.close()
        return None

    invited_user = conn.execute(
        "SELECT username FROM users WHERE telegram_id = ? LIMIT 1",
        (invited_telegram_id,),
    ).fetchone()
    invited_username = str(invited_user["username"] or "").strip() if invited_user else ""

    conn.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)",
        (referrer_telegram_id, None),
    )
    referrer_user = conn.execute(
        "SELECT subscription_end FROM users WHERE telegram_id = ? LIMIT 1",
        (referrer_telegram_id,),
    ).fetchone()

    now_at = dt.datetime.now()
    current_end = parse_date(referrer_user["subscription_end"] if referrer_user else None)
    base = current_end if current_end and current_end > now_at else now_at
    new_end = base + dt.timedelta(days=REFERRAL_REWARD_DAYS)
    new_end_str = new_end.strftime("%Y-%m-%d %H:%M:%S")

    conn.execute(
        "UPDATE users SET subscription_end = ? WHERE telegram_id = ?",
        (new_end_str, referrer_telegram_id),
    )
    try:
        conn.execute(
            """
            INSERT INTO referral_rewards (
                order_id, referrer_telegram_id, invited_telegram_id, paid_days, reward_days, rewarded_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                referrer_telegram_id,
                invited_telegram_id,
                paid_days,
                REFERRAL_REWARD_DAYS,
                now_str(),
            ),
        )
    except sqlite3.IntegrityError:
        conn.close()
        return None

    conn.commit()
    conn.close()
    return {
        "order_id": order_id,
        "referrer_telegram_id": referrer_telegram_id,
        "invited_telegram_id": invited_telegram_id,
        "invited_username": invited_username,
        "paid_days": paid_days,
        "reward_days": REFERRAL_REWARD_DAYS,
        "referrer_new_end": new_end_str,
    }


def apply_loyalty_reward_for_paid_order(order_id: str) -> dict[str, Any] | None:
    if not LOYALTY_ENABLED:
        return None

    conn = get_conn()
    payment = conn.execute(
        """
        SELECT id, order_id, telegram_id, status
        FROM payments
        WHERE order_id = ?
        LIMIT 1
        """,
        (order_id,),
    ).fetchone()
    if not payment or str(payment["status"] or "") != "paid":
        conn.close()
        return None

    telegram_id = int(payment["telegram_id"] or 0)
    if telegram_id <= 0:
        conn.close()
        return None

    already_rewarded = conn.execute(
        "SELECT 1 FROM loyalty_rewards WHERE order_id = ? LIMIT 1",
        (order_id,),
    ).fetchone()
    if already_rewarded:
        conn.close()
        return None

    paid_count_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM payments
        WHERE telegram_id = ?
          AND status = 'paid'
          AND id <= ?
        """,
        (telegram_id, int(payment["id"])),
    ).fetchone()
    paid_count = int(paid_count_row["cnt"] or 0) if paid_count_row else 0
    if paid_count <= 0 or paid_count % LOYALTY_EVERY_PAID != 0:
        conn.close()
        return None

    user = conn.execute(
        "SELECT subscription_end FROM users WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    ).fetchone()
    current_end = parse_date(user["subscription_end"] if user else None)
    now_at = dt.datetime.now()
    base = current_end if current_end and current_end > now_at else now_at
    new_end = (base + dt.timedelta(days=LOYALTY_BONUS_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

    conn.execute(
        "UPDATE users SET subscription_end = ? WHERE telegram_id = ?",
        (new_end, telegram_id),
    )
    conn.execute(
        """
        INSERT INTO loyalty_rewards (
            order_id, telegram_id, paid_count, reward_days, rewarded_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            order_id,
            telegram_id,
            paid_count,
            LOYALTY_BONUS_DAYS,
            now_str(),
        ),
    )
    conn.commit()
    conn.close()
    return {
        "order_id": order_id,
        "telegram_id": telegram_id,
        "paid_count": paid_count,
        "reward_days": LOYALTY_BONUS_DAYS,
        "new_end": new_end,
    }


def create_support_ticket(
    telegram_id: int,
    username: str | None,
    message_text: str,
    *,
    media_kind: str = "text",
    media_file_id: str | None = None,
) -> int:
    now = now_str()
    conn = get_conn()
    cursor = conn.execute(
        """
        INSERT INTO support_tickets (
            telegram_id, username, initial_message, status, created_at, updated_at
        )
        VALUES (?, ?, ?, 'open', ?, ?)
        """,
        (telegram_id, username, message_text, now, now),
    )
    ticket_id = int(cursor.lastrowid)
    conn.execute(
        """
        INSERT INTO support_ticket_messages (
            ticket_id, sender_role, sender_id, message, media_kind, media_file_id, created_at
        )
        VALUES (?, 'user', ?, ?, ?, ?, ?)
        """,
        (ticket_id, telegram_id, message_text, media_kind, media_file_id, now),
    )
    conn.commit()
    conn.close()
    return ticket_id


def add_support_ticket_message(
    ticket_id: int,
    sender_role: str,
    sender_id: int | None,
    message_text: str,
    *,
    media_kind: str = "text",
    media_file_id: str | None = None,
) -> None:
    now = now_str()
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO support_ticket_messages (
            ticket_id, sender_role, sender_id, message, media_kind, media_file_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (ticket_id, sender_role, sender_id, message_text, media_kind, media_file_id, now),
    )
    conn.execute(
        "UPDATE support_tickets SET updated_at = ? WHERE id = ?",
        (now, ticket_id),
    )
    conn.commit()
    conn.close()


def get_support_ticket(ticket_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            id,
            telegram_id,
            username,
            initial_message,
            status,
            assigned_admin_id,
            created_at,
            updated_at,
            taken_at,
            closed_at,
            closed_by
        FROM support_tickets
        WHERE id = ?
        """,
        (ticket_id,),
    ).fetchone()
    conn.close()
    return row


def get_latest_open_support_ticket_for_user(telegram_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            id,
            telegram_id,
            username,
            initial_message,
            status,
            assigned_admin_id,
            created_at,
            updated_at,
            taken_at,
            closed_at,
            closed_by
        FROM support_tickets
        WHERE telegram_id = ? AND status != 'closed'
        ORDER BY id DESC
        LIMIT 1
        """,
        (telegram_id,),
    ).fetchone()
    conn.close()
    return row


def get_support_ticket_messages(ticket_id: int, limit: int = 20, newest_first: bool = False) -> list[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT id, ticket_id, sender_role, sender_id, message, media_kind, media_file_id, created_at
        FROM support_ticket_messages
        WHERE ticket_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (ticket_id, max(1, int(limit))),
    ).fetchall()
    conn.close()
    if newest_first:
        return list(rows)
    return list(reversed(rows))


def get_support_ticket_message_count(ticket_id: int) -> int:
    conn = get_conn()
    row = conn.execute(
        "SELECT COUNT(1) AS cnt FROM support_ticket_messages WHERE ticket_id = ?",
        (ticket_id,),
    ).fetchone()
    conn.close()
    return int(row["cnt"] or 0) if row else 0


def get_support_ticket_last_message(ticket_id: int) -> sqlite3.Row | None:
    rows = get_support_ticket_messages(ticket_id, limit=1, newest_first=True)
    return rows[0] if rows else None


def support_ticket_message_actor(sender_role: str, sender_id: int | None) -> str:
    role = str(sender_role or "").strip().lower()
    if role == "user":
        return "Пользователь"
    if role == "admin":
        if sender_id:
            return f"Админ {admin_label(int(sender_id))}"
        return "Администратор"
    return "Система"


def support_ticket_message_text(row: sqlite3.Row) -> str:
    sender_role = str(row["sender_role"] or "").strip().lower()
    raw = str(row["message"] or "").strip()
    if sender_role == "system" and raw.startswith("ticket_taken_by:"):
        admin_part = raw.split(":", maxsplit=1)[1].strip()
        if admin_part.isdigit():
            return f"Тикет взял администратор {admin_label(int(admin_part))}."
        return "Тикет взят администратором."
    return raw or "Без текста."


def support_ticket_message_preview(row: sqlite3.Row | None, max_len: int = 140) -> str:
    if not row:
        return "—"
    actor = support_ticket_message_actor(str(row["sender_role"] or ""), row["sender_id"])
    message_text = support_ticket_message_text(row)
    media_kind = str(row["media_kind"] or "text").strip().lower()
    if media_kind in {"photo", "document"}:
        message_text = f"🖼 {message_text}"
    compact = " ".join(message_text.split())
    if len(compact) > max_len:
        compact = f"{compact[: max_len - 1]}…"
    created_at = str(row["created_at"] or "-")
    return f"{actor}: {compact} ({created_at})"


def clear_admin_chat_mode_for_ticket(ticket_id: int) -> None:
    for admin_id, chat_ticket_id in list(ADMIN_REPLY_TICKET_BY_ADMIN.items()):
        if int(chat_ticket_id) == int(ticket_id):
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(admin_id, None)


def clear_user_chat_mode_for_ticket(ticket_id: int) -> None:
    for telegram_id, chat_ticket_id in list(USER_ACTIVE_TICKET_CHAT_BY_USER.items()):
        if int(chat_ticket_id) == int(ticket_id):
            USER_ACTIVE_TICKET_CHAT_BY_USER.pop(telegram_id, None)
            USER_TICKET_CHAT_DISABLED.discard(telegram_id)


def clear_ticket_chat_modes(ticket_id: int) -> None:
    clear_admin_chat_mode_for_ticket(ticket_id)
    clear_user_chat_mode_for_ticket(ticket_id)


def take_support_ticket(ticket_id: int, admin_id: int) -> tuple[bool, str]:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return False, "ticket_not_found"
    if str(ticket["status"]) == "closed":
        return False, "ticket_closed"

    assigned_admin = ticket["assigned_admin_id"]
    if assigned_admin and int(assigned_admin) == admin_id:
        return True, "already_taken_by_you"
    if assigned_admin and int(assigned_admin) != admin_id:
        return False, "already_taken_by_other"

    now = now_str()
    conn = get_conn()
    conn.execute(
        """
        UPDATE support_tickets
        SET assigned_admin_id = ?, status = 'in_progress', taken_at = ?, updated_at = ?
        WHERE id = ? AND assigned_admin_id IS NULL
        """,
        (admin_id, now, now, ticket_id),
    )
    conn.commit()
    conn.close()
    return True, "taken"


def ensure_support_ticket_in_progress(ticket_id: int, admin_id: int) -> tuple[sqlite3.Row | None, bool]:
    ticket = get_support_ticket(ticket_id)
    if not ticket or str(ticket["status"]) == "closed":
        return ticket, False

    assigned_admin = ticket["assigned_admin_id"]
    if assigned_admin:
        return ticket, False

    now = now_str()
    conn = get_conn()
    conn.execute(
        """
        UPDATE support_tickets
        SET assigned_admin_id = ?, status = 'in_progress', taken_at = ?, updated_at = ?
        WHERE id = ? AND assigned_admin_id IS NULL
        """,
        (admin_id, now, now, ticket_id),
    )
    conn.commit()
    conn.close()
    return get_support_ticket(ticket_id), True


def close_support_ticket(
    ticket_id: int,
    admin_id: int | None = None,
    *,
    closed_by: str = "admin",
) -> tuple[bool, str]:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return False, "ticket_not_found"
    if str(ticket["status"]) == "closed":
        return False, "already_closed"

    now = now_str()
    close_actor = "user" if str(closed_by).strip().lower() == "user" else "admin"
    conn = get_conn()
    conn.execute(
        """
        UPDATE support_tickets
        SET status = 'closed',
            closed_at = ?,
            updated_at = ?,
            closed_by = ?,
            assigned_admin_id = COALESCE(assigned_admin_id, ?)
        WHERE id = ?
        """,
        (now, now, close_actor, admin_id, ticket_id),
    )
    conn.commit()
    conn.close()
    return True, "closed"


def admin_label(admin_id: int, fallback_username: str | None = None) -> str:
    username = str(fallback_username or "").strip().lstrip("@")
    if not username:
        row = get_user(admin_id)
        if row and row["username"]:
            username = str(row["username"]).strip().lstrip("@")
    return f"@{username}" if username else f"id:{admin_id}"


def support_ticket_admin_text(ticket: sqlite3.Row, assigned_label: str | None = None) -> str:
    username = str(ticket["username"] or "").strip()
    username_line = f"@{username}" if username else "-"
    status = str(ticket["status"] or "open")
    assigned_admin_id = ticket["assigned_admin_id"]
    closed_by = str(ticket["closed_by"] or "").strip().lower()
    updated_at = str(ticket["updated_at"] or ticket["created_at"] or "-")
    message_count = get_support_ticket_message_count(int(ticket["id"]))
    last_message = support_ticket_message_preview(get_support_ticket_last_message(int(ticket["id"])))

    if status == "closed":
        if closed_by == "user":
            status_line = "🔒 Закрыт пользователем"
        elif closed_by == "admin":
            status_line = "🔒 Закрыт администратором"
        else:
            status_line = "🔒 Тикет закрыт"
    elif assigned_admin_id:
        assignee = assigned_label or admin_label(int(assigned_admin_id))
        status_line = f"👤 Тикет взял администратор {assignee}"
    else:
        status_line = "⏳ Ожидает администратора"

    return (
        f"🎫 Тикет #{ticket['id']}\n"
        f"Пользователь: {username_line}\n"
        f"Telegram ID: {ticket['telegram_id']}\n"
        f"Создан: {ticket['created_at']}\n"
        f"Обновлен: {updated_at}\n"
        f"Статус: {status_line}\n"
        f"Сообщений: {message_count}\n"
        f"Последнее: {last_message}"
    )


def serialize_user_for_admin(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None
    subscription_end = row["subscription_end"]
    return {
        "telegram_id": int(row["telegram_id"]),
        "username": row["username"],
        "subscription_end": subscription_end,
        "subscription_active": has_active_subscription(subscription_end),
        "vless_uuid": row["vless_uuid"],
        "trial_used": int(row["trial_used"] or 0),
    }


def admin_search_users(query: str, limit: int = 20) -> list[dict[str, Any]]:
    q = str(query or "").strip()
    if not q:
        return []

    conn = get_conn()
    rows: list[sqlite3.Row]
    if q.isdigit():
        rows = conn.execute(
            """
            SELECT telegram_id, username, subscription_end, vless_uuid, trial_used
            FROM users
            WHERE telegram_id = ?
            LIMIT 1
            """,
            (int(q),),
        ).fetchall()
    else:
        normalized = q.lstrip("@").lower()
        rows = conn.execute(
            """
            SELECT telegram_id, username, subscription_end, vless_uuid, trial_used
            FROM users
            WHERE LOWER(COALESCE(username, '')) LIKE ?
            ORDER BY
                CASE WHEN LOWER(COALESCE(username, '')) = ? THEN 0 ELSE 1 END,
                telegram_id DESC
            LIMIT ?
            """,
            (f"%{normalized}%", normalized, max(1, min(limit, 100))),
        ).fetchall()
    conn.close()
    return [serialized for serialized in (serialize_user_for_admin(row) for row in rows) if serialized]


def set_user_subscription_expired(telegram_id: int) -> str:
    expired_at = (dt.datetime.now() - dt.timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute(
        "UPDATE users SET subscription_end = ? WHERE telegram_id = ?",
        (expired_at, telegram_id),
    )
    conn.commit()
    conn.close()
    return expired_at


async def admin_grant_subscription_days(telegram_id: int, days: int) -> tuple[dict[str, Any] | None, str]:
    grant_days = _clamp(_safe_int(days, 0), 1, 3650)
    user = get_user(telegram_id)
    if not user:
        upsert_user(telegram_id, None)
        user = get_user(telegram_id)
    if not user:
        return None, "user_not_found"

    update_user_subscription(telegram_id, grant_days)
    updated = get_user(telegram_id)
    if not updated:
        return None, "user_not_found"

    try:
        await ensure_vless_uuid(
            telegram_id=telegram_id,
            existing_uuid=updated["vless_uuid"],
            subscription_end=updated["subscription_end"],
        )
    except Exception as exc:  # noqa: BLE001
        return serialize_user_for_admin(updated), f"xui_sync_error:{exc}"

    refreshed = get_user(telegram_id)
    return serialize_user_for_admin(refreshed), "ok"


async def admin_remove_subscription(telegram_id: int) -> tuple[dict[str, Any] | None, str]:
    user = get_user(telegram_id)
    if not user:
        return None, "user_not_found"

    set_user_subscription_expired(telegram_id)
    updated = get_user(telegram_id)
    if not updated:
        return None, "user_not_found"

    try:
        await ensure_vless_uuid(
            telegram_id=telegram_id,
            existing_uuid=updated["vless_uuid"],
            subscription_end=updated["subscription_end"],
        )
    except Exception as exc:  # noqa: BLE001
        return serialize_user_for_admin(updated), f"xui_sync_error:{exc}"

    refreshed = get_user(telegram_id)
    return serialize_user_for_admin(refreshed), "ok"


def calculate_plan_price_for_user(
    telegram_id: int,
    plan: Plan,
) -> tuple[int, dict[str, Any] | None]:
    active_promo = get_user_active_promocode(telegram_id)
    if not active_promo:
        return int(plan.amount_rub), None

    amount = int(plan.amount_rub)
    discount = clamp_discount_to_amount(amount, int(active_promo["discount_rub"]))
    if discount <= 0:
        return amount, None

    return amount - discount, {
        "code": active_promo["code"],
        "discount_rub": discount,
        "expires_at": active_promo["expires_at"],
    }


def normalize_payment_provider(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {SECONDARY_PROVIDER, "secondary", "reserve", "fallback"}:
        return SECONDARY_PROVIDER
    if raw in {"cryptobot", "crypto", "crypto_bot", CRYPTOBOT_PROVIDER}:
        return CRYPTOBOT_PROVIDER
    if raw in {"lzt", "lolz", "lolzpay", "lztmarket", LZT_PROVIDER}:
        return LZT_PROVIDER
    if raw in {"donatepay", DONATEPAY_PROVIDER}:
        return DONATEPAY_PROVIDER
    return DONATEPAY_PROVIDER


def get_active_payment_provider() -> str:
    preferred = normalize_payment_provider(PAYMENT_PROVIDER_PREFERRED)
    if PAYMENT_PROVIDER_PREFERRED and payment_provider_is_ready(preferred):
        return preferred
    if payment_provider_is_ready(DONATEPAY_PROVIDER):
        return DONATEPAY_PROVIDER
    if payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        return CRYPTOBOT_PROVIDER
    if payment_provider_is_ready(LZT_PROVIDER):
        return LZT_PROVIDER
    if payment_provider_is_ready(SECONDARY_PROVIDER):
        return SECONDARY_PROVIDER
    return DONATEPAY_PROVIDER


def payment_provider_label(provider: str | None = None) -> str:
    normalized = normalize_payment_provider(provider)
    if normalized == SECONDARY_PROVIDER:
        return SECONDARY_PAYMENT_LABEL
    if normalized == CRYPTOBOT_PROVIDER:
        return "CryptoBot"
    if normalized == LZT_PROVIDER:
        return "LZT Market"
    return "DonatePay"


def payment_provider_is_ready(provider: str | None = None) -> bool:
    if provider is None or str(provider).strip() == "":
        return bool(
            (DONATEPAY_DONATE_BASE_URL and DONATEPAY_API_KEY)
            or (CRYPTOBOT_ENABLED and CRYPTOBOT_API_TOKEN)
            or (LZT_ENABLED and LZT_API_TOKEN and LZT_MERCHANT_ID > 0)
            or (SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL)
        )
    normalized = normalize_payment_provider(provider)
    if normalized == SECONDARY_PROVIDER:
        return bool(SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL)
    if normalized == CRYPTOBOT_PROVIDER:
        return bool(CRYPTOBOT_ENABLED and CRYPTOBOT_API_TOKEN)
    if normalized == LZT_PROVIDER:
        return bool(LZT_ENABLED and LZT_API_TOKEN and LZT_MERCHANT_ID > 0)
    return bool(DONATEPAY_DONATE_BASE_URL and DONATEPAY_API_KEY)


def payment_order_cooldown_left(telegram_id: int) -> int:
    if ORDER_CREATE_COOLDOWN_SECONDS <= 0:
        return 0

    conn = get_conn()
    row = conn.execute(
        """
        SELECT created_at
        FROM payments
        WHERE telegram_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(telegram_id),),
    ).fetchone()
    conn.close()
    if not row:
        return 0

    created_at = parse_date(str(row["created_at"] or ""))
    if not created_at:
        return 0

    elapsed = int((dt.datetime.now() - created_at).total_seconds())
    left = ORDER_CREATE_COOLDOWN_SECONDS - elapsed
    return max(0, left)


def cancel_pending_orders_for_user(telegram_id: int, reason: str = "replaced_by_new_order") -> int:
    conn = get_conn()
    cursor = conn.execute(
        """
        UPDATE payments
        SET status = 'cancelled', raw_payload = ?
        WHERE telegram_id = ? AND status = 'pending'
        """,
        (json.dumps({"reason": reason, "cancelled_at": now_str()}, ensure_ascii=False), telegram_id),
    )
    changed = int(cursor.rowcount or 0)
    conn.commit()
    conn.close()
    return changed


def create_payment_order(
    telegram_id: int,
    plan: Plan,
    amount_rub: int,
    promo_code: str | None = None,
    promo_discount_rub: int = 0,
) -> str:
    cancel_expired_payments(telegram_id)
    cancel_pending_orders_for_user(telegram_id)
    order_id = f"BV-{telegram_id}-{uuid.uuid4().hex[:8].upper()}"
    provider = get_active_payment_provider()
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO payments (
            order_id, telegram_id, provider, amount_rub, days, plan_code, base_amount_rub,
            promo_code, promo_discount_rub, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            order_id,
            telegram_id,
            provider,
            float(amount_rub),
            int(plan.days),
            plan.code,
            float(plan.amount_rub),
            normalize_promo_code(promo_code or "") or None,
            max(0, int(promo_discount_rub)),
            now_str(),
        ),
    )
    conn.commit()
    conn.close()
    return order_id


def cancel_expired_payments(telegram_id: int | None = None) -> int:
    cutoff = (dt.datetime.now() - payment_ttl()).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    cursor = conn.cursor()

    if telegram_id is None:
        cursor.execute(
            """
            UPDATE payments
            SET status = 'cancelled', raw_payload = COALESCE(raw_payload, ?)
            WHERE status = 'pending' AND created_at <= ?
            """,
            (json.dumps({"reason": "expired"}, ensure_ascii=False), cutoff),
        )
    else:
        cursor.execute(
            """
            UPDATE payments
            SET status = 'cancelled', raw_payload = COALESCE(raw_payload, ?)
            WHERE status = 'pending' AND telegram_id = ? AND created_at <= ?
            """,
            (json.dumps({"reason": "expired"}, ensure_ascii=False), telegram_id, cutoff),
        )

    changed = int(cursor.rowcount or 0)
    conn.commit()
    conn.close()
    return changed


def get_payment(order_id: str, apply_expiry: bool = True) -> sqlite3.Row | None:
    if apply_expiry:
        cancel_expired_payments()
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            id,
            order_id,
            telegram_id,
            provider,
            amount_rub,
            days,
            plan_code,
            base_amount_rub,
            promo_code,
            promo_discount_rub,
            status,
            created_at,
            paid_at
        FROM payments
        WHERE order_id = ?
        """,
        (order_id,),
    ).fetchone()
    conn.close()
    return row


def cancel_payment_order(
    order_id: str,
    telegram_id: int | None = None,
    reason: str = "cancelled_by_user",
) -> tuple[bool, str]:
    payment = get_payment(order_id, apply_expiry=True)
    if not payment:
        return False, "order_not_found"

    if telegram_id is not None and int(payment["telegram_id"]) != telegram_id:
        return False, "forbidden"

    if payment["status"] != "pending":
        return False, f"not_pending:{payment['status']}"

    conn = get_conn()
    conn.execute(
        """
        UPDATE payments
        SET status = 'cancelled', raw_payload = ?
        WHERE order_id = ? AND status = 'pending'
        """,
        (
            json.dumps({"reason": reason, "cancelled_at": now_str()}, ensure_ascii=False),
            order_id,
        ),
    )
    conn.commit()
    conn.close()
    return True, "cancelled"


def recreate_payment_order_from_previous(
    source_order_id: str,
    telegram_id: int,
    *,
    username: str | None = None,
) -> tuple[bool, str, dict[str, Any] | None]:
    source_order = get_payment(source_order_id, apply_expiry=True)
    if not source_order:
        return False, "order_not_found", None
    if int(source_order["telegram_id"] or 0) != int(telegram_id):
        return False, "forbidden", None
    if str(source_order["status"] or "").strip().lower() not in {"cancelled", "paid"}:
        return False, "order_not_recreatable", None

    plan = resolve_plan_from_payment_row(source_order)
    plan_replaced = False
    if not plan:
        requested_days = max(1, int(source_order["days"] or 1))
        available_plans = list(get_active_plans().values())
        if not available_plans:
            return False, "plan_not_found", None
        available_plans.sort(key=lambda item: (abs(int(item.days) - requested_days), int(item.days)))
        plan = available_plans[0]
        plan_replaced = True

    final_amount, promo = calculate_plan_price_for_user(telegram_id, plan)
    new_order_id = create_payment_order(
        telegram_id=telegram_id,
        plan=plan,
        amount_rub=final_amount,
        promo_code=promo["code"] if promo else None,
        promo_discount_rub=promo["discount_rub"] if promo else 0,
    )
    payment = get_payment(new_order_id, apply_expiry=False)
    provider = payment["provider"] if payment else get_active_payment_provider()
    payment_url = build_payment_url(
        new_order_id,
        final_amount,
        provider,
        telegram_id=telegram_id,
        username=username,
    )
    return True, "ok", {
        "order_id": new_order_id,
        "source_order_id": str(source_order["order_id"] or "").strip().upper(),
        "plan": plan,
        "plan_replaced": plan_replaced,
        "amount_rub": int(final_amount),
        "provider": normalize_payment_provider(provider),
        "provider_label": payment_provider_label(provider),
        "payment_url": payment_url,
        "promo": promo,
        "expires_at": payment_expires_at_str(payment["created_at"] if payment else now_str()),
    }


def get_latest_pending_payment(telegram_id: int) -> sqlite3.Row | None:
    cancel_expired_payments(telegram_id)
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            order_id,
            provider,
            amount_rub,
            days,
            plan_code,
            base_amount_rub,
            promo_code,
            promo_discount_rub,
            created_at
        FROM payments
        WHERE telegram_id = ? AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (telegram_id,),
    ).fetchone()
    conn.close()
    return row


def get_latest_recreatable_payment(telegram_id: int) -> sqlite3.Row | None:
    cancel_expired_payments(telegram_id)
    conn = get_conn()
    row = conn.execute(
        """
        SELECT order_id, telegram_id, provider, amount_rub, days, plan_code, status, created_at, paid_at
        FROM payments
        WHERE telegram_id = ? AND status IN ('cancelled', 'paid')
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(telegram_id),),
    ).fetchone()
    conn.close()
    return row


def find_pending_order_by_amount(
    amount: float,
    tolerance: float = 0.01,
    telegram_id: int | None = None,
) -> str | None:
    cancel_expired_payments()
    target = float(amount)
    tol = abs(float(tolerance))
    conn = get_conn()
    if telegram_id is None:
        rows = conn.execute(
            """
            SELECT order_id
            FROM payments
            WHERE status = 'pending' AND ABS(amount_rub - ?) <= ?
            ORDER BY id DESC
            LIMIT 20
            """,
            (target, tol),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT order_id
            FROM payments
            WHERE status = 'pending' AND telegram_id = ? AND ABS(amount_rub - ?) <= ?
            ORDER BY id DESC
            LIMIT 20
            """,
            (int(telegram_id), target, tol),
        ).fetchall()
    conn.close()

    if rows:
        return str(rows[0]["order_id"])
    return None


def get_single_pending_order() -> str | None:
    cancel_expired_payments()
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT order_id
        FROM payments
        WHERE status = 'pending'
        ORDER BY id DESC
        LIMIT 2
        """
    ).fetchall()
    conn.close()
    if len(rows) == 1:
        return str(rows[0]["order_id"])
    return None


def has_pending_orders_for_provider(provider: str) -> bool:
    normalized = normalize_payment_provider(provider)
    conn = get_conn()
    row = conn.execute(
        """
        SELECT 1
        FROM payments
        WHERE status = 'pending' AND provider = ?
        LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    conn.close()
    return bool(row)


def resolve_order_id_from_payload(payload: dict[str, Any]) -> tuple[str | None, str]:
    order_id = extract_order_id(payload)
    if order_id:
        return order_id, "order_id"

    telegram_id = extract_telegram_id(payload)
    if telegram_id:
        pending = get_latest_pending_payment(telegram_id)
        if pending:
            return str(pending["order_id"]), "telegram_id"

    incoming_amount = extract_amount(payload)
    if incoming_amount is not None:
        order_id = find_pending_order_by_amount(
            float(incoming_amount),
            telegram_id=telegram_id,
        )
        if order_id:
            return order_id, "amount"

    order_id = get_single_pending_order()
    if order_id:
        return order_id, "single_pending"

    return None, "not_found"


def consume_promocode_for_paid_order(order_id: str) -> None:
    conn = get_conn()
    payment = conn.execute(
        """
        SELECT telegram_id, promo_code, promo_discount_rub, status
        FROM payments
        WHERE order_id = ?
        """,
        (order_id,),
    ).fetchone()
    if not payment:
        conn.close()
        return

    promo_code = normalize_promo_code(str(payment["promo_code"] or ""))
    promo_discount = int(payment["promo_discount_rub"] or 0)
    if payment["status"] != "paid" or not promo_code or promo_discount <= 0:
        conn.close()
        return

    activation = conn.execute(
        """
        SELECT id
        FROM promo_activations
        WHERE telegram_id = ? AND code = ? AND used_at IS NULL
        ORDER BY activated_at DESC
        LIMIT 1
        """,
        (int(payment["telegram_id"]), promo_code),
    ).fetchone()
    if activation:
        conn.execute(
            """
            UPDATE promo_activations
            SET used_at = ?, used_order_id = ?
            WHERE id = ?
            """,
            (now_str(), order_id, int(activation["id"])),
        )
        conn.commit()

    conn.close()


def build_main_keyboard(telegram_id: int | None = None) -> ReplyKeyboardMarkup:
    keyboard_rows: list[list[KeyboardButton]] = [
        [
            KeyboardButton(text="🚀 Подключить VPN"),
            KeyboardButton(text="💳 Купить подписку"),
        ],
        [KeyboardButton(text="👤 Личный кабинет"), KeyboardButton(text="📚 Инструкции")],
        [KeyboardButton(text="🛟 Поддержка"), KeyboardButton(text="📜 Правила")],
        [KeyboardButton(text="🔥 Акции")],
    ]
    if REFERRAL_ENABLED:
        keyboard_rows.append([KeyboardButton(text="👥 Реферальная программа")])
    if telegram_id is not None and is_admin_user(telegram_id):
        keyboard_rows.append([KeyboardButton(text="🛠 Админ")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard_rows,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие",
    )


def build_support_ticket_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=SUPPORT_EXIT_BUTTON),
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Опишите проблему или отправьте изображение",
    )


def build_sale_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if TRIAL_ENABLED:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🎁 Тест на {TRIAL_DAYS} {day_word(TRIAL_DAYS)}",
                    callback_data="sale:trial",
                )
            ]
        )
    if REFERRAL_ENABLED:
        rows.append(
            [
                InlineKeyboardButton(
                    text="👥 Реферальная программа",
                    callback_data="sale:referral",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_referral_keyboard(referral_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📤 Поделиться ссылкой", url=build_referral_share_url(referral_link))],
            [InlineKeyboardButton(text="🔥 Открыть акции", callback_data="sale:open")],
        ]
    )


def build_plan_keyboard(plans: dict[str, Plan]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=plan_line(plan),
                callback_data=f"buy:{plan.code}",
            )
        ]
        for plan in plans.values()
    ]
    rows.append([InlineKeyboardButton(text="📚 Как подключить", callback_data="guides:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_payment_keyboard(
    payment_url: str,
    order_id: str,
    provider: str | None = None,
    donatepay_url: str = "",
    cryptobot_url: str = "",
    lzt_url: str = "",
    secondary_payment_url: str = "",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    normalized_provider = normalize_payment_provider(provider)
    mini_app_url = build_webapp_order_url(order_id, auto_pay=False)
    donatepay_url_clean = str(donatepay_url or "").strip()
    cryptobot_url_clean = str(cryptobot_url or "").strip()
    lzt_url_clean = str(lzt_url or "").strip()
    if mini_app_url and (donatepay_url_clean or cryptobot_url_clean or lzt_url_clean):
        rows.append(
            [
                InlineKeyboardButton(
                    text="🧩 Выбрать оплату в Mini App",
                    web_app=WebAppInfo(url=mini_app_url),
                )
            ]
        )
    primary_label = payment_provider_label(provider)
    if payment_url:
        if normalized_provider == DONATEPAY_PROVIDER:
            primary_label = "DonatePay"
        elif normalized_provider == CRYPTOBOT_PROVIDER:
            primary_label = "CryptoBot"
        elif normalized_provider == LZT_PROVIDER:
            primary_label = "LZT Market"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"💸 Оплатить в {primary_label}",
                    url=payment_url,
                )
            ]
        )
    if donatepay_url_clean and donatepay_url_clean != str(payment_url or "").strip():
        rows.append(
            [
                InlineKeyboardButton(
                    text="💸 Оплатить в DonatePay",
                    url=donatepay_url_clean,
                )
            ]
        )
    if (
        cryptobot_url_clean
        and (normalized_provider != CRYPTOBOT_PROVIDER or cryptobot_url_clean != str(payment_url or "").strip())
    ):
        rows.append(
            [
                InlineKeyboardButton(
                    text="₿ Оплатить в CryptoBot",
                    url=cryptobot_url_clean,
                )
            ]
        )
    if (
        lzt_url_clean
        and (normalized_provider != LZT_PROVIDER or lzt_url_clean != str(payment_url or "").strip())
    ):
        rows.append(
            [
                InlineKeyboardButton(
                    text="⚡ Оплатить в LZT Market",
                    url=lzt_url_clean,
                )
            ]
        )
    if SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL:
        fallback_url = str(secondary_payment_url or "").strip()
        if not fallback_url:
            payment_row = get_payment(order_id, apply_expiry=False)
            fallback_amount = int(round(float(payment_row["amount_rub"]))) if payment_row else 0
            fallback_telegram_id = (
                int(payment_row["telegram_id"]) if payment_row and payment_row["telegram_id"] else None
            )
            fallback_username: str | None = None
            if fallback_telegram_id:
                fallback_user = get_user(fallback_telegram_id)
                fallback_username = str(fallback_user["username"] or "").strip() if fallback_user else None
            fallback_url = build_secondary_payment_url(
                order_id=order_id,
                amount_rub=fallback_amount,
                telegram_id=fallback_telegram_id,
                username=fallback_username,
            )
        if fallback_url:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"🪙 {SECONDARY_PAYMENT_LABEL}",
                        url=fallback_url,
                    )
                ]
            )

    rows.extend(
        [
            [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"paycheck:{order_id}")],
            [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"paycancel:{order_id}")],
            [InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_order_closed_keyboard(order_id: str | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    clean_order_id = str(order_id or "").strip().upper()
    if clean_order_id:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔁 Повторить заказ",
                    callback_data=f"payrepeat:{clean_order_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")])
    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


def build_profile_keyboard(subscription_active: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if subscription_active:
        rows.append(
            [InlineKeyboardButton(text="🔄 Перевыпустить ключ", callback_data="profile:reissue")],
        )
    if payment_provider_is_ready():
        rows.append(
            [
                InlineKeyboardButton(text="💳 Продлить на 7 дней", callback_data="profile:renew7"),
                InlineKeyboardButton(text="💳 Продлить на 30 дней", callback_data="profile:renew30"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(text="💳 Продлить на 90 дней", callback_data="profile:renew90"),
                InlineKeyboardButton(text="💳 Продлить на 365 дней", callback_data="profile:renew365"),
            ]
        )
        rows.append([InlineKeyboardButton(text="🔁 Повторить прошлый тариф", callback_data="profile:renew_last")])
        rows.append([InlineKeyboardButton(text="🛰 Переключить на резерв", callback_data="profile:toggle_route")])
    rows.extend(
        [
            [InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="profile:promo")],
            [InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_subscription_reminder_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if payment_provider_is_ready():
        rows.append(
            [
                InlineKeyboardButton(text="💳 Продлить 7 дней", callback_data="profile:renew7"),
                InlineKeyboardButton(text="💳 Продлить 30 дней", callback_data="profile:renew30"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(text="💳 Продлить 90 дней", callback_data="profile:renew90"),
                InlineKeyboardButton(text="💳 Продлить 365 дней", callback_data="profile:renew365"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(text="🔁 Повторить прошлый тариф", callback_data="profile:renew_last"),
            ]
        )
    webapp_plans_url = build_webapp_tab_url("plans")
    if webapp_plans_url:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🧩 Продлить в Mini App",
                    web_app=WebAppInfo(url=webapp_plans_url),
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_webapp_open_keyboard(tab: str = "plans") -> InlineKeyboardMarkup | None:
    webapp_url = build_webapp_tab_url(tab)
    if not webapp_url:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧩 Открыть Mini App",
                    web_app=WebAppInfo(url=webapp_url),
                )
            ]
        ]
    )


def build_ticket_admin_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Взять тикет", callback_data=f"tkt_take:{ticket_id}"),
                InlineKeyboardButton(text="💬 Ответить", callback_data=f"tkt_reply:{ticket_id}"),
            ],
            [InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"tkt_close:{ticket_id}")],
        ]
    )


def apply_order_status_to_text(original_text: str, status_text: str) -> str:
    base = (original_text or "").split(ORDER_STATUS_MARKER, maxsplit=1)[0].rstrip()
    return f"{base}{ORDER_STATUS_MARKER}{status_text}"


def build_guides_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Android • Happ", callback_data="guide:android_happ")],
            [InlineKeyboardButton(text="🤖 Android • V2rayTun", callback_data="guide:android_v2raytun")],
            [InlineKeyboardButton(text="🍏 iOS • Happ", callback_data="guide:ios_happ")],
            [InlineKeyboardButton(text="🪟 Windows • V2rayTun", callback_data="guide:windows_v2raytun")],
            [InlineKeyboardButton(text="🍎 macOS • Happ", callback_data="guide:macos_happ")],
            [InlineKeyboardButton(text="🐧 Linux • V2rayTun", callback_data="guide:linux_v2raytun")],
        ]
    )


def build_onboarding_keyboard(telegram_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if APP_LINK_HAPP_ANDROID:
        rows.append([InlineKeyboardButton(text="🤖 Скачать Happ (Android)", url=APP_LINK_HAPP_ANDROID)])
    if APP_LINK_HAPP_IOS:
        rows.append([InlineKeyboardButton(text="🍏 Скачать Happ (iOS)", url=APP_LINK_HAPP_IOS)])
    if APP_LINK_HAPP_WINDOWS:
        rows.append([InlineKeyboardButton(text="🪟 Скачать Happ (Windows)", url=APP_LINK_HAPP_WINDOWS)])
    if APP_LINK_HAPP_MACOS:
        rows.append([InlineKeyboardButton(text="🍎 Скачать Happ (macOS)", url=APP_LINK_HAPP_MACOS)])
    if APP_LINK_V2RAYTUN_ANDROID:
        rows.append([InlineKeyboardButton(text="📱 V2rayTun Android", url=APP_LINK_V2RAYTUN_ANDROID)])
    if APP_LINK_V2RAYTUN_IOS:
        rows.append([InlineKeyboardButton(text="📱 V2rayTun iOS", url=APP_LINK_V2RAYTUN_IOS)])
    profile_url = build_subscription_profile_url(telegram_id)
    if profile_url:
        rows.append([InlineKeyboardButton(text="🔗 Открыть Subscription URL", url=profile_url)])
    rows.append([InlineKeyboardButton(text="✅ Я подключился", callback_data="onboarding:done")])
    rows.append([InlineKeyboardButton(text="🛟 Нужна помощь", callback_data="guides:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_subscription_delivery_keyboard(telegram_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    profile_url = build_subscription_profile_url(telegram_id)
    if profile_url:
        rows.append([InlineKeyboardButton(text="🔗 Открыть Subscription URL", url=profile_url)])
    rows.append([InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_panel_keyboard() -> InlineKeyboardMarkup:
    manual_mode = update_notify_manual_mode_enabled()
    maintenance_enabled = maintenance_mode_enabled()
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=(
                    "🟠 Режим обновлений: ручной"
                    if manual_mode
                    else "🟢 Режим обновлений: авто"
                ),
                callback_data="adminctl:toggle_update_mode",
            )
        ],
        [
            InlineKeyboardButton(
                text=(
                    "🔴 Технические работы: ВКЛ"
                    if maintenance_enabled
                    else "🟢 Технические работы: ВЫКЛ"
                ),
                callback_data="adminctl:toggle_maintenance",
            )
        ],
        [InlineKeyboardButton(text="📣 Отправить update-уведомление", callback_data="adminctl:send_update")],
        [InlineKeyboardButton(text="🔄 Обновить панель", callback_data="adminctl:refresh")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_admin_panel_text() -> str:
    update_mode = "ручной" if update_notify_manual_mode_enabled() else "авто"
    maintenance_status = "включены" if maintenance_mode_enabled() else "выключены"
    return (
        "🛠 Админ-панель\n"
        "Команды:\n"
        "• /prices — текущие цены и скидки\n"
        "• /admin_stats — метрики и конверсия\n"
        "• /sale_notify — рассылка акции из pricing.json\n"
        "• /sale_notify <текст> — рассылка своего текста\n"
        "• /promo CODE — проверить активацию промокода как пользователь\n"
        "• /blacklist_add <tg_id> [причина] — добавить в blacklist\n"
        "• /blacklist_del <tg_id> — убрать из blacklist\n"
        "• /blacklist_list — список blacklist\n"
        "• /flags_list — подозрительные антифрод-флаги\n"
        "• /flags_resolve <id> — пометить флаг обработанным\n"
        "• /backup_now — создать резервную копию сейчас\n"
        "• /myid — показать ваш Telegram ID\n\n"
        f"Режим уведомлений об обновлениях: {update_mode}\n"
        f"Технические работы: {maintenance_status}\n"
        "Промокоды создаются в Mini App админ-панели.\n"
        f"Авто-отмена неоплаченных заказов: {PAYMENT_PENDING_TTL_MINUTES} минут."
    )


def build_webapp_order_url(order_id: str, auto_pay: bool = False) -> str:
    base = (WEBAPP_PUBLIC_URL or "").strip()
    if not base:
        return ""
    separator = "&" if "?" in base else "?"
    order_arg = quote(str(order_id).strip(), safe="")
    auto_arg = "1" if auto_pay else "0"
    return f"{base}{separator}order_id={order_arg}&autopay={auto_arg}"


def build_webapp_tab_url(tab: str) -> str:
    base = (WEBAPP_PUBLIC_URL or "").strip()
    if not base:
        return ""
    normalized_tab = str(tab or "").strip().lower()
    allowed_tabs = {"account", "plans", "order", "admin", "broadcast"}
    if normalized_tab not in allowed_tabs:
        normalized_tab = "plans"
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}tab={quote(normalized_tab, safe='')}"


def _append_query_params(base: str, params: dict[str, str]) -> str:
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}{urlencode(params)}"


def build_payment_success_url(order_id: str, provider: str | None = None) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    base = resolved_public_base_url()
    if not base:
        base = "https://connect.boxvolt.shop"

    success_url = f"{base}/pay/success"
    params: dict[str, str] = {}
    if clean_order_id:
        params["order_id"] = clean_order_id
        params["start"] = build_order_start_payload(clean_order_id)
    normalized_provider = normalize_payment_provider(provider)
    if provider and normalized_provider:
        params["provider"] = normalized_provider
    if not params:
        return success_url
    return _append_query_params(success_url, params)


def cryptobot_invoice_meta_key(order_id: str) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    return f"cryptobot_invoice:{clean_order_id}"


def _as_int(value: Any) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def cryptobot_extract_invoice_url(invoice_payload: dict[str, Any]) -> str:
    for key in ("pay_url", "web_app_invoice_url", "mini_app_invoice_url", "bot_invoice_url"):
        value = str(invoice_payload.get(key) or "").strip()
        if value:
            return value
    return ""


def cryptobot_parse_invoice_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    invoice_id = _as_int(value.get("invoice_id"))
    payload = str(value.get("payload") or "").strip().upper()
    status = str(value.get("status") or "").strip().lower()
    amount = str(value.get("amount") or "").strip()
    invoice = {
        "invoice_id": invoice_id,
        "status": status,
        "payload": payload,
        "amount": amount,
        "asset": str(value.get("asset") or "").strip().upper(),
        "fiat": str(value.get("fiat") or "").strip().upper(),
        "pay_url": str(value.get("pay_url") or "").strip(),
        "web_app_invoice_url": str(value.get("web_app_invoice_url") or "").strip(),
        "mini_app_invoice_url": str(value.get("mini_app_invoice_url") or "").strip(),
        "bot_invoice_url": str(value.get("bot_invoice_url") or "").strip(),
        "created_at": now_str(),
    }
    if not invoice["pay_url"]:
        invoice["pay_url"] = cryptobot_extract_invoice_url(value)
    return invoice


def get_cached_cryptobot_invoice(order_id: str) -> dict[str, Any] | None:
    raw = get_app_meta(cryptobot_invoice_meta_key(order_id))
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    invoice = cryptobot_parse_invoice_payload(payload)
    if not invoice.get("invoice_id"):
        return None
    return invoice


def save_cached_cryptobot_invoice(order_id: str, invoice_payload: dict[str, Any]) -> None:
    invoice = cryptobot_parse_invoice_payload(invoice_payload)
    if not invoice.get("invoice_id"):
        return
    if not invoice.get("payload"):
        invoice["payload"] = str(order_id or "").strip().upper()
    set_app_meta(
        cryptobot_invoice_meta_key(order_id),
        json.dumps(invoice, ensure_ascii=False),
    )


async def cryptobot_api_call(
    method: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        raise RuntimeError("cryptobot_not_configured")

    url = f"{CRYPTOBOT_API_BASE}/{str(method).strip().lstrip('/')}"
    headers = {
        "Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN,
        "Accept": "application/json",
    }
    query: dict[str, str] = {}
    for key, value in (params or {}).items():
        if value is None:
            continue
        query[str(key)] = str(value)

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=query, headers=headers)

    if response.status_code == 429:
        raise RuntimeError("cryptobot_rate_limited")
    if response.status_code >= 400:
        body = response.text.strip().replace("\n", " ")[:250]
        raise RuntimeError(f"cryptobot_http_{response.status_code}: {body}")

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"cryptobot_invalid_json: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("cryptobot_invalid_payload")
    if not payload.get("ok"):
        error_name = str(payload.get("error") or payload.get("name") or "cryptobot_api_error")
        raise RuntimeError(error_name)
    result = payload.get("result")
    if isinstance(result, dict):
        return result
    return {"result": result}


def build_cryptobot_description(order_id: str, amount_rub: int) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    amount = max(1, int(amount_rub))
    return f"{CRYPTOBOT_DESCRIPTION_PREFIX} • {amount} {CRYPTOBOT_FIAT} • {clean_order_id}"


async def ensure_cryptobot_invoice_url(
    order_id: str,
    amount_rub: int,
    *,
    telegram_id: int | None = None,
    username: str | None = None,
    force_refresh: bool = False,
) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    amount = max(1, int(amount_rub))
    if not clean_order_id or not payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        return ""

    cached = get_cached_cryptobot_invoice(clean_order_id)
    if cached and not force_refresh:
        cached_url = cryptobot_extract_invoice_url(cached)
        if cached_url and str(cached.get("status") or "").lower() not in {"expired", "invalid"}:
            return cached_url

    payer_tag, username_clean = payment_identity_values(telegram_id, username)
    params: dict[str, Any] = {
        "currency_type": "fiat",
        "fiat": CRYPTOBOT_FIAT,
        "amount": str(amount),
        "description": build_cryptobot_description(clean_order_id, amount),
        "payload": clean_order_id,
        "expires_in": str(CRYPTOBOT_INVOICE_EXPIRES_SECONDS),
        "allow_comments": "false",
        "allow_anonymous": "false",
    }
    if payer_tag:
        params["hidden_message"] = f"Order {clean_order_id} • {payer_tag}"
    elif username_clean:
        params["hidden_message"] = f"Order {clean_order_id} • @{username_clean}"

    result = await cryptobot_api_call("createInvoice", params=params)
    invoice = cryptobot_parse_invoice_payload(result)
    if not invoice.get("invoice_id"):
        raise RuntimeError("cryptobot_invoice_missing_id")
    if not invoice.get("payload"):
        invoice["payload"] = clean_order_id
    save_cached_cryptobot_invoice(clean_order_id, invoice)
    return cryptobot_extract_invoice_url(invoice)


def build_cryptobot_url_from_cache(order_id: str) -> str:
    cached = get_cached_cryptobot_invoice(order_id)
    if not cached:
        return ""
    return cryptobot_extract_invoice_url(cached)


def lzt_invoice_meta_key(order_id: str) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    return f"lzt_invoice:{clean_order_id}"


def lzt_extract_invoice_url(invoice_payload: dict[str, Any]) -> str:
    if not isinstance(invoice_payload, dict):
        return ""
    for key in ("url", "invoice_url", "pay_url"):
        value = str(invoice_payload.get(key) or "").strip()
        if value:
            return value
    return ""


def lzt_parse_invoice_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    invoice_source = value.get("invoice")
    if isinstance(invoice_source, dict):
        value = invoice_source
    invoice_id = _as_int(value.get("invoice_id"))
    payment_id = str(value.get("payment_id") or "").strip().upper()
    status = str(value.get("status") or "").strip().lower()
    invoice = {
        "invoice_id": invoice_id,
        "payment_id": payment_id,
        "status": status,
        "amount": value.get("amount"),
        "merchant_id": _as_int(value.get("merchant_id")),
        "payer_user_id": _as_int(value.get("payer_user_id")),
        "is_test": bool(value.get("is_test")),
        "url": str(value.get("url") or "").strip(),
        "comment": str(value.get("comment") or "").strip(),
        "additional_data": str(value.get("additional_data") or "").strip(),
        "invoice_date": _as_int(value.get("invoice_date")),
        "paid_date": _as_int(value.get("paid_date")),
        "expires_at": _as_int(value.get("expires_at")),
        "raw": value,
        "created_at": now_str(),
    }
    if not invoice["url"]:
        invoice["url"] = lzt_extract_invoice_url(value)
    return invoice


def get_cached_lzt_invoice(order_id: str) -> dict[str, Any] | None:
    raw = get_app_meta(lzt_invoice_meta_key(order_id))
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    invoice = lzt_parse_invoice_payload(payload)
    if not invoice.get("invoice_id"):
        return None
    return invoice


def save_cached_lzt_invoice(order_id: str, invoice_payload: dict[str, Any]) -> None:
    invoice = lzt_parse_invoice_payload(invoice_payload)
    if not invoice.get("invoice_id"):
        return
    if not invoice.get("payment_id"):
        invoice["payment_id"] = str(order_id or "").strip().upper()
    set_app_meta(
        lzt_invoice_meta_key(order_id),
        json.dumps(invoice, ensure_ascii=False),
    )


def build_lzt_callback_url() -> str:
    base = resolved_public_base_url()
    if not base:
        return ""
    webhook_path = _normalize_http_path(LZT_WEBHOOK_PATH, "/lzt/webhook")
    callback = f"{base}{webhook_path}"
    if LZT_WEBHOOK_SECRET:
        callback = _append_query_params(callback, {"secret": LZT_WEBHOOK_SECRET})
    return callback


def build_lzt_success_url(order_id: str) -> str:
    return build_payment_success_url(order_id, provider=LZT_PROVIDER)


def build_lzt_description(order_id: str, amount_rub: int) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    amount = max(1, int(amount_rub))
    return f"{LZT_DESCRIPTION_PREFIX} • {amount} RUB • {clean_order_id}"


async def lzt_api_call(
    endpoint: str,
    *,
    http_method: str = "GET",
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not payment_provider_is_ready(LZT_PROVIDER):
        raise RuntimeError("lzt_not_configured")

    clean_endpoint = str(endpoint or "").strip().lstrip("/")
    url = f"{LZT_API_BASE}/{clean_endpoint}"
    headers = {
        "Authorization": f"Bearer {LZT_API_TOKEN}",
        "Accept": "application/json",
    }
    method = str(http_method or "GET").strip().upper()
    async with httpx.AsyncClient(timeout=20.0) as client:
        if method == "POST":
            response = await client.post(url, headers={**headers, "Content-Type": "application/json"}, json=payload)
        else:
            query: dict[str, str] = {}
            for key, value in (params or {}).items():
                if value is None:
                    continue
                query[str(key)] = str(value)
            response = await client.get(url, headers=headers, params=query)

    if response.status_code == 429:
        raise RuntimeError("lzt_rate_limited")
    if response.status_code >= 400:
        body = response.text.strip().replace("\n", " ")[:250]
        raise RuntimeError(f"lzt_http_{response.status_code}: {body}")

    try:
        result = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"lzt_invalid_json: {exc}") from exc
    if not isinstance(result, dict):
        raise RuntimeError("lzt_invalid_payload")
    errors = result.get("errors")
    if isinstance(errors, list) and errors:
        joined = "; ".join(str(item) for item in errors[:3])
        raise RuntimeError(f"lzt_api_error: {joined}")
    return result


async def ensure_lzt_invoice_url(
    order_id: str,
    amount_rub: int,
    *,
    telegram_id: int | None = None,
    username: str | None = None,
    force_refresh: bool = False,
) -> str:
    clean_order_id = str(order_id or "").strip().upper()
    amount = max(1, int(amount_rub))
    if not clean_order_id or not payment_provider_is_ready(LZT_PROVIDER):
        return ""

    cached = get_cached_lzt_invoice(clean_order_id)
    if cached and not force_refresh:
        cached_url = lzt_extract_invoice_url(cached)
        if cached_url and str(cached.get("status") or "").lower() not in {"cancelled", "expired", "closed"}:
            return cached_url

    payer_tag, username_clean = payment_identity_values(telegram_id, username)
    payload: dict[str, Any] = {
        "currency": LZT_CURRENCY,
        "amount": amount,
        "payment_id": clean_order_id,
        "comment": clean_order_id,
        "url_success": build_lzt_success_url(clean_order_id),
        "merchant_id": LZT_MERCHANT_ID,
    }
    if LZT_MERCHANT_KEY:
        payload["merchant_key"] = LZT_MERCHANT_KEY
    callback_url = build_lzt_callback_url()
    if callback_url:
        payload["url_callback"] = callback_url
    if payer_tag:
        payload["additional_data"] = payer_tag
    elif username_clean:
        payload["additional_data"] = f"@{username_clean}"

    result = await lzt_api_call("invoice", http_method="POST", payload=payload)
    invoice = lzt_parse_invoice_payload(result)
    if not invoice.get("invoice_id"):
        raise RuntimeError("lzt_invoice_missing_id")
    if not invoice.get("payment_id"):
        invoice["payment_id"] = clean_order_id
    save_cached_lzt_invoice(clean_order_id, invoice)
    return lzt_extract_invoice_url(invoice)


def build_lzt_url_from_cache(order_id: str) -> str:
    cached = get_cached_lzt_invoice(order_id)
    if not cached:
        return ""
    return lzt_extract_invoice_url(cached)


def build_donatepay_url(
    order_id: str,
    amount_rub: int,
    telegram_id: int | None = None,
    username: str | None = None,
) -> str:
    base = DONATEPAY_DONATE_BASE_URL
    if not base:
        return ""

    clean_order_id = str(order_id).strip().upper()
    amount_text = str(int(amount_rub))
    amount_dot = f"{int(amount_rub)}.00"
    payer_tag, username_clean = payment_identity_values(telegram_id, username)
    params = {
        "amount": amount_text,
        "sum": amount_text,
        "value": amount_text,
        "price": amount_text,
        "amount_rub": amount_text,
        "amount_float": amount_dot,
        "currency": "RUB",
        "comment": clean_order_id,
        "message": clean_order_id,
        "text": clean_order_id,
        "description": clean_order_id,
        "order_id": clean_order_id,
        "utm_source": "boxvolt_bot",
        "utm_medium": "telegram",
        "utm_campaign": clean_order_id,
    }
    if payer_tag:
        params["nickname"] = payer_tag
        params["name"] = payer_tag
        params["user"] = payer_tag
    if username_clean:
        params["username"] = username_clean
        params["vars[username]"] = username_clean
    if telegram_id is not None and int(telegram_id) > 0:
        params["telegram_id"] = str(int(telegram_id))
        params["vars[telegram_id]"] = str(int(telegram_id))
    return _append_query_params(base, params)


def build_secondary_payment_url(
    order_id: str,
    amount_rub: int,
    telegram_id: int | None = None,
    username: str | None = None,
) -> str:
    base = str(SECONDARY_PAYMENT_URL or "").strip()
    if not base:
        return ""

    clean_order_id = str(order_id).strip().upper()
    params: dict[str, str] = {
        "order_id": clean_order_id,
        "comment": clean_order_id,
        "message": clean_order_id,
        "source": "boxvolt_bot",
    }
    if amount_rub > 0:
        params["amount"] = str(int(amount_rub))
        params["sum"] = str(int(amount_rub))
        params["currency"] = "RUB"

    payer_tag, username_clean = payment_identity_values(telegram_id, username)
    if payer_tag:
        params["nickname"] = payer_tag
    if username_clean:
        params["username"] = username_clean
    if telegram_id is not None and int(telegram_id) > 0:
        params["telegram_id"] = str(int(telegram_id))
    return _append_query_params(base, params)


def build_payment_url(
    order_id: str,
    amount_rub: int,
    provider: str | None = None,
    telegram_id: int | None = None,
    username: str | None = None,
) -> str:
    normalized = normalize_payment_provider(provider)
    if normalized == CRYPTOBOT_PROVIDER:
        return build_cryptobot_url_from_cache(order_id)
    if normalized == LZT_PROVIDER:
        return build_lzt_url_from_cache(order_id)
    if normalized == SECONDARY_PROVIDER:
        return build_secondary_payment_url(
            order_id,
            amount_rub,
            telegram_id=telegram_id,
            username=username,
        )
    return build_donatepay_url(
        order_id,
        amount_rub,
        telegram_id=telegram_id,
        username=username,
    )


async def build_payment_links_for_order(
    order_id: str,
    amount_rub: int,
    provider: str | None = None,
    *,
    telegram_id: int | None = None,
    username: str | None = None,
) -> dict[str, str]:
    normalized = normalize_payment_provider(provider)
    donatepay_url = ""
    cryptobot_url = ""
    lzt_url = ""

    if payment_provider_is_ready(DONATEPAY_PROVIDER):
        donatepay_url = build_donatepay_url(
            order_id,
            amount_rub,
            telegram_id=telegram_id,
            username=username,
        )
    if payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        try:
            cryptobot_url = await ensure_cryptobot_invoice_url(
                order_id,
                amount_rub,
                telegram_id=telegram_id,
                username=username,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[cryptobot] Failed to create invoice for {order_id}: {exc}")
            cryptobot_url = build_cryptobot_url_from_cache(order_id)
    if payment_provider_is_ready(LZT_PROVIDER):
        try:
            lzt_url = await ensure_lzt_invoice_url(
                order_id,
                amount_rub,
                telegram_id=telegram_id,
                username=username,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[lzt] Failed to create invoice for {order_id}: {exc}")
            lzt_url = build_lzt_url_from_cache(order_id)

    payment_url = ""
    if normalized == CRYPTOBOT_PROVIDER:
        payment_url = cryptobot_url or donatepay_url or lzt_url
    elif normalized == LZT_PROVIDER:
        payment_url = lzt_url or donatepay_url or cryptobot_url
    elif normalized == SECONDARY_PROVIDER:
        payment_url = ""
    else:
        payment_url = donatepay_url or cryptobot_url or lzt_url

    secondary_url = (
        build_secondary_payment_url(
            order_id,
            amount_rub,
            telegram_id=telegram_id,
            username=username,
        )
        if SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL
        else ""
    )
    return {
        "payment_url": str(payment_url or "").strip(),
        "donatepay_payment_url": str(donatepay_url or "").strip(),
        "cryptobot_payment_url": str(cryptobot_url or "").strip(),
        "lzt_payment_url": str(lzt_url or "").strip(),
        "secondary_payment_url": str(secondary_url or "").strip(),
    }


def build_donation_url(
    order_id: str,
    amount_rub: int,
    telegram_id: int | None = None,
    username: str | None = None,
) -> str:
    # Backward-compatible wrapper.
    return build_payment_url(
        order_id,
        amount_rub,
        telegram_id=telegram_id,
        username=username,
    )


def _normalize_http_path(path: str, fallback: str) -> str:
    cleaned = (path or "").strip()
    if not cleaned:
        cleaned = fallback
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned


def resolved_public_base_url() -> str:
    direct = (PUBLIC_BASE_URL or "").strip().rstrip("/")
    if direct:
        return direct

    for candidate in (WEBAPP_PUBLIC_URL,):
        raw = (candidate or "").strip()
        if not raw:
            continue
        parsed = urlsplit(raw)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _subscription_secret_bytes() -> bytes:
    secret = (SUBSCRIPTION_SECRET or BOT_TOKEN).encode("utf-8")
    return secret


def _base36_encode(number: int) -> str:
    if number < 0:
        raise ValueError("base36 only supports non-negative integers")
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if number == 0:
        return "0"
    value = number
    output: list[str] = []
    while value > 0:
        value, rem = divmod(value, 36)
        output.append(alphabet[rem])
    return "".join(reversed(output))


def _base36_decode(value: str) -> int | None:
    token = str(value or "").strip().lower()
    if not token or not re.fullmatch(r"[0-9a-z]+", token):
        return None
    try:
        return int(token, 36)
    except ValueError:
        return None


def build_legacy_subscription_token(telegram_id: int) -> str:
    payload = f"sub:{telegram_id}".encode("utf-8")
    return hmac.new(_subscription_secret_bytes(), payload, hashlib.sha256).hexdigest()[:32]


def normalize_subscription_uuid(user_uuid: str | None) -> str:
    value = str(user_uuid or "").strip().lower()
    return value or "-"


def build_subscription_token(telegram_id: int, user_uuid: str | None = None) -> str:
    prefix = _base36_encode(int(telegram_id))
    uuid_part = normalize_subscription_uuid(user_uuid)
    payload = f"sub:v2:{telegram_id}:{uuid_part}".encode("utf-8")
    digest = hmac.new(_subscription_secret_bytes(), payload, hashlib.sha256).hexdigest()[:20]
    return f"{prefix}-{digest}"


def extract_telegram_id_from_subscription_token(token: str) -> int | None:
    raw = str(token or "").strip().lower()
    if "-" not in raw:
        return None
    prefix = raw.split("-", 1)[0].strip()
    telegram_id = _base36_decode(prefix)
    if telegram_id is None or telegram_id <= 0:
        return None
    return telegram_id


def is_valid_subscription_token(
    telegram_id: int,
    token: str,
    *,
    user_uuid: str | None = None,
) -> bool:
    provided = (token or "").strip().lower()
    if not provided:
        return False

    expected_v2 = build_subscription_token(telegram_id, user_uuid=user_uuid)
    if hmac.compare_digest(provided, expected_v2):
        return True

    expected_legacy = build_legacy_subscription_token(telegram_id)
    return hmac.compare_digest(provided, expected_legacy)


def build_subscription_url(telegram_id: int, user_uuid: str | None = None) -> str:
    base = resolved_public_base_url()
    if not base:
        return ""
    path = _normalize_http_path(SUBSCRIPTION_PATH, "/sub").rstrip("/")
    if not path:
        path = "/sub"
    if user_uuid is None:
        row = get_user(telegram_id)
        user_uuid = str(row["vless_uuid"] or "").strip() if row else None
    token = build_subscription_token(telegram_id, user_uuid=user_uuid)
    return f"{base}{path}/{token}"


def build_subscription_profile_url(telegram_id: int) -> str:
    sub_url = build_subscription_url(telegram_id)
    if not sub_url:
        return ""
    return f"{sub_url}/profile"


def gigabytes_to_bytes(total_gb: int) -> int:
    if total_gb <= 0:
        return 0
    return total_gb * 1024 * 1024 * 1024


def non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def subscription_total_bytes() -> int:
    if XUI_TOTAL_GB > 0:
        return gigabytes_to_bytes(XUI_TOTAL_GB)
    if SUBSCRIPTION_DISPLAY_TOTAL_GB > 0:
        return gigabytes_to_bytes(SUBSCRIPTION_DISPLAY_TOTAL_GB)
    return 0


def subscription_expire_unix(subscription_end: str | None) -> int:
    parsed = parse_date(subscription_end)
    if not parsed:
        return 0
    return max(0, int(parsed.timestamp()))


def build_profile_title_header() -> str:
    title = str(SUBSCRIPTION_PROFILE_TITLE or "").strip() or "BoxVolt Технология 3.0"
    encoded = base64.b64encode(title.encode("utf-8")).decode("ascii")
    return f"base64:{encoded}"


def build_subscription_status_header(subscription_end: str | None) -> str:
    expiry = parse_date(subscription_end)
    expiry_text = expiry.strftime("%d.%m.%Y %H:%M") if expiry else "-"
    active = has_active_subscription(subscription_end)
    status_text = "✅ Active" if active else "❌ Inactive"
    lines = [
        f"{status_text} / Осталось: {format_subscription_remaining(subscription_end)}",
        f"📅 Истекает: {expiry_text}",
        f"🔄 Автообновление: {SUBSCRIPTION_UPDATE_INTERVAL_HOURS} ч.",
    ]
    if SUPPORT_CONTACT:
        lines.append(f"🛟 Поддержка: {SUPPORT_CONTACT}")
    return ";".join(lines)


def build_subscription_text_block(telegram_id: int) -> str:
    sub_url = build_subscription_url(telegram_id)
    if not sub_url:
        return ""
    return (
        "🔄 URL-подписка для Happ/V2rayTun (автообновление):\n"
        f"{as_copyable_key(sub_url)}\n"
        "Импортируйте как Subscription URL."
    )


def build_subscription_qr_data_url(subscription_url: str) -> str:
    text = str(subscription_url or "").strip()
    if not text:
        return ""
    try:
        import qrcode

        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=8,
            border=1,
        )
        qr.add_data(text)
        qr.make(fit=True)
        img = qr.make_image(fill_color="#eaf2ff", back_color="#081225")
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        return f"data:image/png;base64,{encoded}"
    except Exception as exc:  # noqa: BLE001
        print(f"[qr] Failed to build subscription QR: {exc}")
        return ""


def donatepay_item_id(item: dict[str, Any]) -> int:
    for key in ("id", "transaction_id", "donate_id"):
        raw = item.get(key)
        try:
            return int(str(raw))
        except (TypeError, ValueError):
            continue
    return 0


def donatepay_event_is_success(item: dict[str, Any]) -> bool:
    status = str(item.get("status") or "").lower()
    what = str(item.get("what") or item.get("type") or "").lower()

    if status:
        if any(
            word in status
            for word in ("wait", "pending", "process", "created", "new", "hold", "in_progress")
        ):
            return False
        if any(word in status for word in ("fail", "cancel", "refund", "reject", "decline")):
            return False
        if status in {"success", "paid", "completed", "done", "succeeded"}:
            return True
    if not status and what and any(word in what for word in ("donate", "donation", "payment", "pay")):
        return True
    return False


def build_donatepay_payload(item: dict[str, Any]) -> dict[str, Any]:
    vars_block = item.get("vars")
    if not isinstance(vars_block, dict):
        vars_block = {}

    comment = str(item.get("comment") or vars_block.get("comment") or "").strip()
    message = str(item.get("message") or comment or vars_block.get("text") or "").strip()
    telegram_hint = str(vars_block.get("telegram_id") or "").strip()

    metadata: dict[str, Any] = {}
    if telegram_hint.isdigit():
        metadata["telegram_id"] = int(telegram_hint)

    return {
        "provider": DONATEPAY_PROVIDER,
        "event": "donation",
        "status": item.get("status") or "paid",
        "amount": item.get("sum") or item.get("amount"),
        "sum": item.get("sum") or item.get("amount"),
        "message": message,
        "comment": comment,
        "metadata": metadata,
        "data": item,
    }


async def fetch_donatepay_transactions() -> list[dict[str, Any]]:
    if not DONATEPAY_API_KEY:
        return []

    params = {
        "access_token": DONATEPAY_API_KEY,
        "limit": str(DONATEPAY_POLL_LIMIT),
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            f"{DONATEPAY_API_BASE}/transactions",
            params=params,
            headers={"Accept": "application/json"},
        )

    if response.status_code == 429:
        return []
    if response.status_code >= 400:
        body = response.text.strip().replace("\n", " ")[:250]
        raise RuntimeError(f"donatepay_transactions_http_{response.status_code}: {body}")

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"donatepay_transactions_invalid_json: {exc}") from exc

    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


async def process_donatepay_sync(target_order_id: str | None = None) -> dict[str, int]:
    global DONATEPAY_LAST_TRANSACTION_ID

    transactions = await fetch_donatepay_transactions()
    if not transactions:
        return {"checked": 0, "matched": 0, "processed": 0}

    checked = 0
    matched = 0
    processed = 0
    max_seen = DONATEPAY_LAST_TRANSACTION_ID
    target_order = target_order_id.strip().upper() if target_order_id else ""
    target_payment = get_payment(target_order, apply_expiry=False) if target_order else None
    target_amount = float(target_payment["amount_rub"]) if target_payment else None
    seen_fingerprints: set[str] = set()

    for item in sorted(transactions, key=donatepay_item_id):
        fingerprint = "|".join(
            [
                str(item.get("id") or item.get("transaction_id") or item.get("uuid") or ""),
                str(item.get("created_at") or ""),
                str(item.get("sum") or item.get("amount") or ""),
                str(item.get("comment") or ""),
                str(item.get("status") or ""),
            ]
        )
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)

        tx_id = donatepay_item_id(item)
        if tx_id > 0 and tx_id <= DONATEPAY_LAST_TRANSACTION_ID and not target_order:
            continue
        if tx_id > max_seen:
            max_seen = tx_id
        if not donatepay_event_is_success(item):
            continue

        checked += 1
        payload = build_donatepay_payload(item)

        if target_order:
            extracted = extract_order_id(payload)
            if extracted and extracted.upper() != target_order:
                continue
            incoming_amount = extract_amount(payload)
            if (
                not extracted
                and incoming_amount is not None
                and target_amount is not None
                and abs(float(incoming_amount) - float(target_amount)) > 0.01
            ):
                continue
            order_id = target_order
            match_reason = "target_order"
        else:
            order_id, match_reason = resolve_order_id_from_payload(payload)
            if not order_id:
                continue

        matched += 1
        ok, reason = await process_paid_order(order_id, payload)
        if ok:
            if reason == "already_paid":
                continue
            processed += 1
            print(f"[dp-poll] Order {order_id} processed ({reason}, by={match_reason})")
        else:
            print(f"[dp-poll] Order {order_id} skipped ({reason}, by={match_reason})")

    if max_seen > DONATEPAY_LAST_TRANSACTION_ID:
        DONATEPAY_LAST_TRANSACTION_ID = max_seen

    return {"checked": checked, "matched": matched, "processed": processed}


async def donatepay_poll_loop() -> None:
    interval = max(5, DONATEPAY_POLL_INTERVAL_SECONDS)
    while True:
        try:
            if not DONATEPAY_API_KEY:
                await asyncio.sleep(interval)
                continue

            stats = await process_donatepay_sync()
            if stats["processed"] > 0:
                print(f"[dp-poll] Processed {stats['processed']} payment(s) this cycle")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[dp-poll] Sync error: {exc}")

        await asyncio.sleep(interval)


def cryptobot_item_invoice_id(item: dict[str, Any]) -> int:
    return _as_int(item.get("invoice_id"))


def cryptobot_item_is_paid(item: dict[str, Any]) -> bool:
    status = str(item.get("status") or "").strip().lower()
    return status in {"paid", "confirmed"}


def build_cryptobot_payload(item: dict[str, Any], event: str = "invoice_paid") -> dict[str, Any]:
    payload_order = str(item.get("payload") or item.get("invoice_payload") or "").strip().upper()
    metadata: dict[str, Any] = {"invoice_id": _as_int(item.get("invoice_id"))}
    telegram_hint = extract_telegram_id({"payload": item})
    if telegram_hint:
        metadata["telegram_id"] = telegram_hint

    return {
        "provider": CRYPTOBOT_PROVIDER,
        "event": event,
        "status": item.get("status") or ("paid" if event == "invoice_paid" else ""),
        "amount": item.get("amount") or item.get("paid_amount"),
        "sum": item.get("amount") or item.get("paid_amount"),
        "order_id": payload_order,
        "payload": payload_order,
        "metadata": metadata,
        "data": item,
    }


def validate_cryptobot_webhook_secret(request: web.Request, payload: dict[str, Any]) -> bool:
    if not CRYPTOBOT_WEBHOOK_SECRET:
        return True

    auth_header = str(request.headers.get("Authorization") or "").strip()
    bearer_secret = ""
    if auth_header.lower().startswith("bearer "):
        bearer_secret = auth_header[7:].strip()

    incoming = (
        request.headers.get("X-Webhook-Secret")
        or request.headers.get("X-CryptoBot-Secret")
        or request.headers.get("X-Cryptobot-Secret")
        or bearer_secret
        or request.query.get("secret")
        or str(payload.get("secret") or "")
    )
    return hmac.compare_digest(str(incoming), CRYPTOBOT_WEBHOOK_SECRET)


def validate_cryptobot_signature(request: web.Request) -> bool:
    if not CRYPTOBOT_VALIDATE_SIGNATURE:
        return True
    if not CRYPTOBOT_API_TOKEN:
        return False

    signature = str(
        request.headers.get("Crypto-Pay-API-Signature")
        or request.headers.get("crypto-pay-api-signature")
        or ""
    ).strip().lower()
    if not signature:
        return False

    header_items: list[tuple[str, str]] = []
    for key, value in request.headers.items():
        key_lower = str(key).strip().lower()
        if key_lower == "crypto-pay-api-signature":
            continue
        if not key_lower:
            continue
        header_items.append((key_lower, str(value).strip()))
    header_items.sort(key=lambda item: item[0])
    check_string = "\n".join(f"{key}:{value}" for key, value in header_items)

    secret = hashlib.sha256(CRYPTOBOT_API_TOKEN.encode("utf-8")).digest()
    expected = hmac.new(secret, check_string.encode("utf-8"), hashlib.sha256).hexdigest().lower()
    return hmac.compare_digest(signature, expected)


async def cryptobot_webhook(request: web.Request) -> web.Response:
    try:
        payload = await parse_webhook_payload(request)
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"ok": False, "error": f"invalid_payload:{exc}"}, status=400)

    if not validate_cryptobot_webhook_secret(request, payload):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    if not validate_cryptobot_signature(request):
        return web.json_response({"ok": False, "error": "bad_signature"}, status=401)

    update_type = str(payload.get("update_type") or payload.get("event") or "").strip().lower()
    invoice_payload = payload.get("payload")
    if not isinstance(invoice_payload, dict):
        invoice_payload = payload.get("data")
    if not isinstance(invoice_payload, dict):
        return web.json_response({"ok": True, "ignored": "no_invoice_payload"})

    wrapped_payload = build_cryptobot_payload(invoice_payload, event=update_type or "invoice_update")
    order_hint = extract_order_id(wrapped_payload)
    if order_hint:
        save_cached_cryptobot_invoice(order_hint, invoice_payload)

    if update_type != "invoice_paid" and not cryptobot_item_is_paid(invoice_payload):
        return web.json_response({"ok": True, "ignored": "not_paid_event"})
    if not is_successful_payment(wrapped_payload):
        return web.json_response({"ok": True, "ignored": "not_success_event"})

    order_id, match_reason = resolve_order_id_from_payload(wrapped_payload)
    if order_id and match_reason != "order_id":
        print(f"[cb-webhook] Pending order matched by {match_reason}: {order_id}")
    if not order_id:
        return web.json_response({"ok": False, "error": "order_id_not_found"}, status=400)

    ok, reason = await process_paid_order(order_id, wrapped_payload)
    code = 200 if ok else 400
    return web.json_response({"ok": ok, "reason": reason}, status=code)


def get_pending_cryptobot_invoice_bindings(limit: int = 500) -> dict[int, str]:
    cancel_expired_payments()
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT p.order_id AS order_id, a.value AS meta_value
        FROM payments p
        JOIN app_meta a ON a.key = ('cryptobot_invoice:' || p.order_id)
        WHERE p.status = 'pending'
        ORDER BY p.id DESC
        LIMIT ?
        """,
        (max(1, min(int(limit), 5000)),),
    ).fetchall()
    conn.close()

    result: dict[int, str] = {}
    for row in rows:
        order_id = str(row["order_id"] or "").strip().upper()
        raw = str(row["meta_value"] or "").strip()
        if not order_id or not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        invoice_id = _as_int(payload.get("invoice_id"))
        if invoice_id <= 0:
            continue
        result[invoice_id] = order_id
    return result


async def fetch_cryptobot_invoices(invoice_ids: list[int]) -> list[dict[str, Any]]:
    cleaned = sorted({int(item) for item in invoice_ids if int(item) > 0})
    if not cleaned:
        return []
    result = await cryptobot_api_call(
        "getInvoices",
        params={
            "invoice_ids": ",".join(str(item) for item in cleaned),
            "count": str(min(1000, len(cleaned))),
            "offset": "0",
        },
    )
    items = result.get("items")
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    return []


async def process_cryptobot_sync(target_order_id: str | None = None) -> dict[str, int]:
    if not payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        return {"checked": 0, "matched": 0, "processed": 0}

    target_order = str(target_order_id or "").strip().upper()
    invoice_bindings = get_pending_cryptobot_invoice_bindings(limit=1000)
    if target_order:
        invoice_bindings = {
            invoice_id: order_id
            for invoice_id, order_id in invoice_bindings.items()
            if order_id == target_order
        }
    if not invoice_bindings:
        return {"checked": 0, "matched": 0, "processed": 0}

    checked = 0
    matched = 0
    processed = 0
    invoice_ids = list(invoice_bindings.keys())
    for idx in range(0, len(invoice_ids), 100):
        chunk = invoice_ids[idx : idx + 100]
        items = await fetch_cryptobot_invoices(chunk)
        for item in items:
            invoice_id = cryptobot_item_invoice_id(item)
            if invoice_id <= 0:
                continue
            order_id = invoice_bindings.get(invoice_id)
            if not order_id:
                continue

            checked += 1
            save_cached_cryptobot_invoice(order_id, item)
            if not cryptobot_item_is_paid(item):
                continue
            matched += 1

            wrapped_payload = build_cryptobot_payload(item, event="invoice_paid")
            ok, reason = await process_paid_order(order_id, wrapped_payload)
            if ok:
                if reason == "already_paid":
                    continue
                processed += 1
                print(f"[cb-poll] Order {order_id} processed ({reason})")
            else:
                print(f"[cb-poll] Order {order_id} skipped ({reason})")

    return {"checked": checked, "matched": matched, "processed": processed}


async def cryptobot_poll_loop() -> None:
    interval = max(10, CRYPTOBOT_POLL_INTERVAL_SECONDS)
    while True:
        try:
            if not payment_provider_is_ready(CRYPTOBOT_PROVIDER):
                await asyncio.sleep(interval)
                continue

            stats = await process_cryptobot_sync()
            if stats["processed"] > 0:
                print(f"[cb-poll] Processed {stats['processed']} payment(s) this cycle")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[cb-poll] Sync error: {exc}")

        await asyncio.sleep(interval)


def lzt_item_invoice_id(item: dict[str, Any]) -> int:
    return _as_int(item.get("invoice_id"))


def lzt_item_is_paid(item: dict[str, Any]) -> bool:
    status = str(item.get("status") or "").strip().lower()
    return status in {"paid", "success", "completed"}


def build_lzt_payload(item: dict[str, Any], event: str = "invoice_paid") -> dict[str, Any]:
    payment_id = str(item.get("payment_id") or "").strip().upper()
    metadata: dict[str, Any] = {"invoice_id": _as_int(item.get("invoice_id"))}
    telegram_hint = extract_telegram_id({"payload": item})
    if telegram_hint:
        metadata["telegram_id"] = telegram_hint

    return {
        "provider": LZT_PROVIDER,
        "event": event,
        "status": item.get("status") or ("paid" if event == "invoice_paid" else ""),
        "amount": item.get("amount"),
        "sum": item.get("amount"),
        "order_id": payment_id,
        "payment_id": payment_id,
        "comment": item.get("comment"),
        "metadata": metadata,
        "data": item,
    }


def validate_lzt_webhook_secret(request: web.Request, payload: dict[str, Any]) -> bool:
    if not LZT_WEBHOOK_SECRET:
        return True

    auth_header = str(request.headers.get("Authorization") or "").strip()
    bearer_secret = ""
    if auth_header.lower().startswith("bearer "):
        bearer_secret = auth_header[7:].strip()

    incoming = (
        request.headers.get("X-Webhook-Secret")
        or request.headers.get("X-LZT-Secret")
        or request.headers.get("X-Lolz-Secret")
        or bearer_secret
        or request.query.get("secret")
        or str(payload.get("secret") or "")
    )
    return hmac.compare_digest(str(incoming), LZT_WEBHOOK_SECRET)


async def lzt_webhook(request: web.Request) -> web.Response:
    try:
        payload = await parse_webhook_payload(request)
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"ok": False, "error": f"invalid_payload:{exc}"}, status=400)

    if not validate_lzt_webhook_secret(request, payload):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    invoice_payload = payload.get("invoice")
    if not isinstance(invoice_payload, dict):
        invoice_payload = payload.get("data")
    if not isinstance(invoice_payload, dict):
        invoice_payload = payload.get("payload")
    if not isinstance(invoice_payload, dict):
        invoice_payload = payload if isinstance(payload, dict) else {}
    if not isinstance(invoice_payload, dict) or not invoice_payload:
        return web.json_response({"ok": True, "ignored": "no_invoice_payload"})

    normalized = lzt_parse_invoice_payload(invoice_payload)
    order_hint = str(normalized.get("payment_id") or "").strip().upper()
    if order_hint:
        save_cached_lzt_invoice(order_hint, normalized)

    if not lzt_item_is_paid(normalized):
        return web.json_response({"ok": True, "ignored": "not_paid_event"})

    wrapped_payload = build_lzt_payload(normalized, event=str(payload.get("event") or "invoice_paid"))
    if not is_successful_payment(wrapped_payload):
        return web.json_response({"ok": True, "ignored": "not_success_event"})

    order_id, match_reason = resolve_order_id_from_payload(wrapped_payload)
    if order_id and match_reason != "order_id":
        print(f"[lzt-webhook] Pending order matched by {match_reason}: {order_id}")
    if not order_id:
        return web.json_response({"ok": False, "error": "order_id_not_found"}, status=400)

    ok, reason = await process_paid_order(order_id, wrapped_payload)
    code = 200 if ok else 400
    return web.json_response({"ok": ok, "reason": reason}, status=code)


def get_pending_lzt_invoice_bindings(limit: int = 500) -> dict[str, str]:
    cancel_expired_payments()
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT p.order_id AS order_id, a.value AS meta_value
        FROM payments p
        JOIN app_meta a ON a.key = ('lzt_invoice:' || p.order_id)
        WHERE p.status = 'pending'
        ORDER BY p.id DESC
        LIMIT ?
        """,
        (max(1, min(int(limit), 5000)),),
    ).fetchall()
    conn.close()

    result: dict[str, str] = {}
    for row in rows:
        order_id = str(row["order_id"] or "").strip().upper()
        raw = str(row["meta_value"] or "").strip()
        if not order_id or not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        invoice = lzt_parse_invoice_payload(payload)
        invoice_id = lzt_item_invoice_id(invoice)
        payment_id = str(invoice.get("payment_id") or order_id).strip().upper()
        if invoice_id <= 0 and not payment_id:
            continue
        key = str(invoice_id) if invoice_id > 0 else payment_id
        result[key] = order_id
    return result


async def fetch_lzt_invoice(*, invoice_id: int = 0, payment_id: str = "") -> dict[str, Any]:
    params: dict[str, Any] = {}
    if invoice_id > 0:
        params["invoice_id"] = invoice_id
    elif payment_id:
        params["payment_id"] = str(payment_id).strip().upper()
    else:
        return {}
    result = await lzt_api_call("invoice", params=params)
    return lzt_parse_invoice_payload(result)


async def process_lzt_sync(target_order_id: str | None = None) -> dict[str, int]:
    if not payment_provider_is_ready(LZT_PROVIDER):
        return {"checked": 0, "matched": 0, "processed": 0}

    target_order = str(target_order_id or "").strip().upper()
    bindings = get_pending_lzt_invoice_bindings(limit=1000)
    if target_order:
        bindings = {key: order_id for key, order_id in bindings.items() if order_id == target_order}
    if not bindings:
        return {"checked": 0, "matched": 0, "processed": 0}

    checked = 0
    matched = 0
    processed = 0
    for invoice_ref, order_id in bindings.items():
        invoice_id = _as_int(invoice_ref)
        payment_id = "" if invoice_id > 0 else str(invoice_ref or "").strip().upper()
        try:
            item = await fetch_lzt_invoice(invoice_id=invoice_id, payment_id=payment_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[lzt-poll] Failed to fetch invoice for {order_id}: {exc}")
            continue
        if not item:
            continue

        checked += 1
        save_cached_lzt_invoice(order_id, item)
        if not lzt_item_is_paid(item):
            continue
        matched += 1

        wrapped_payload = build_lzt_payload(item, event="invoice_paid")
        ok, reason = await process_paid_order(order_id, wrapped_payload)
        if ok:
            if reason == "already_paid":
                continue
            processed += 1
            print(f"[lzt-poll] Order {order_id} processed ({reason})")
        else:
            print(f"[lzt-poll] Order {order_id} skipped ({reason})")

    return {"checked": checked, "matched": matched, "processed": processed}


async def lzt_poll_loop() -> None:
    interval = max(10, LZT_POLL_INTERVAL_SECONDS)
    while True:
        try:
            if not payment_provider_is_ready(LZT_PROVIDER):
                await asyncio.sleep(interval)
                continue

            stats = await process_lzt_sync()
            if stats["processed"] > 0:
                print(f"[lzt-poll] Processed {stats['processed']} payment(s) this cycle")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[lzt-poll] Sync error: {exc}")

        await asyncio.sleep(interval)


async def sync_pending_payment_order(order_id: str) -> tuple[bool, str]:
    payment = get_payment(order_id, apply_expiry=True)
    if not payment:
        return False, "order_not_found"
    if payment["status"] == "paid":
        return True, "already_paid"
    if payment["status"] != "pending":
        return False, f"not_pending:{payment['status']}"

    sync_attempted = False
    if DONATEPAY_API_KEY:
        sync_attempted = True
        await process_donatepay_sync(target_order_id=order_id)
    if payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        sync_attempted = True
        await process_cryptobot_sync(target_order_id=order_id)
    if payment_provider_is_ready(LZT_PROVIDER):
        sync_attempted = True
        await process_lzt_sync(target_order_id=order_id)
    if not sync_attempted:
        return False, "payment_sync_not_configured"

    updated = get_payment(order_id, apply_expiry=True)
    if updated and updated["status"] == "paid":
        return True, "paid"
    if updated and updated["status"] != "pending":
        return False, f"not_pending:{updated['status']}"
    return False, "not_paid"


async def payments_cleanup_loop() -> None:
    interval = max(20, PAYMENT_CLEANUP_INTERVAL_SECONDS)
    while True:
        try:
            cancelled = cancel_expired_payments()
            if cancelled > 0:
                print(f"[payments] Auto-cancelled expired orders: {cancelled}")
            if ANTIABUSE_FLAGS_ENABLED:
                cleaned = prune_old_suspicious_flags(ANTIABUSE_FLAG_RETENTION_DAYS)
                if cleaned > 0:
                    print(f"[antiabuse] Pruned old resolved flags: {cleaned}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[payments] Cleanup error: {exc}")

        await asyncio.sleep(interval)


def get_pending_payments_for_reminders(limit: int = 500) -> list[sqlite3.Row]:
    cancel_expired_payments()
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT order_id, telegram_id, provider, amount_rub, created_at
        FROM payments
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (max(1, min(int(limit), 5000)),),
    ).fetchall()
    conn.close()
    return rows


def pending_order_reminder_meta_key(order_id: str, threshold_minutes: int) -> str:
    return f"pending_order_reminder:{str(order_id).strip().upper()}:{int(threshold_minutes)}m"


async def send_pending_payment_reminders() -> None:
    if not PENDING_ORDER_REMINDER_ENABLED or not PENDING_ORDER_REMINDER_MINUTES:
        return

    pending_rows = get_pending_payments_for_reminders(limit=1000)
    if not pending_rows:
        return

    now_at = dt.datetime.now()
    sent = 0
    for row in pending_rows:
        created_at = parse_date(str(row["created_at"] or ""))
        if not created_at:
            continue
        age_minutes = (now_at - created_at).total_seconds() / 60.0
        due_thresholds = [minute for minute in PENDING_ORDER_REMINDER_MINUTES if age_minutes >= minute]
        if not due_thresholds:
            continue
        threshold = max(due_thresholds)
        order_id = str(row["order_id"] or "").strip().upper()
        if not order_id:
            continue

        reminder_key = pending_order_reminder_meta_key(order_id, threshold)
        if get_app_meta(reminder_key):
            continue

        telegram_id = int(row["telegram_id"] or 0)
        if telegram_id <= 0:
            continue
        user = get_user(telegram_id)
        username = str(user["username"] or "").strip() if user else None
        amount_rub = int(round(float(row["amount_rub"] or 0)))
        provider = normalize_payment_provider(str(row["provider"] or ""))
        links = await build_payment_links_for_order(
            order_id,
            amount_rub,
            provider,
            telegram_id=telegram_id,
            username=username,
        )
        expires_at = payment_expires_at_str(str(row["created_at"] or ""))
        provider_label = payment_provider_label(provider)
        text = (
            "⏳ Напоминание по неоплаченному заказу\n"
            f"Код: {order_id}\n"
            f"Сумма: {amount_rub} ₽\n"
            f"Провайдер: {provider_label}\n"
            f"Истекает: {expires_at or '-'}\n\n"
            "Нажмите «Оплатить» или «Проверить оплату»."
        )
        try:
            await bot.send_message(
                telegram_id,
                text,
                reply_markup=build_payment_keyboard(
                    links["payment_url"],
                    order_id,
                    provider,
                    donatepay_url=links["donatepay_payment_url"],
                    cryptobot_url=links["cryptobot_payment_url"],
                    lzt_url=links["lzt_payment_url"],
                    secondary_payment_url=links["secondary_payment_url"],
                ),
            )
            set_app_meta(reminder_key, now_str())
            sent += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[pending-reminder] Failed to notify {telegram_id} for {order_id}: {exc}")

    if sent > 0:
        print(f"[pending-reminder] Sent {sent} reminder(s)")


async def pending_order_reminder_loop() -> None:
    interval = max(30, PENDING_ORDER_REMINDER_INTERVAL_SECONDS)
    while True:
        try:
            if PENDING_ORDER_REMINDER_ENABLED:
                await send_pending_payment_reminders()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[pending-reminder] Loop error: {exc}")
        await asyncio.sleep(interval)


def build_subscription_expiry_reminder_key(telegram_id: int, threshold_hours: int) -> str:
    return f"sub_expiry_reminder:{int(telegram_id)}:{int(threshold_hours)}h"


def pick_subscription_reminder_threshold(hours_left: float) -> int | None:
    if hours_left <= 0:
        return None
    for threshold in SUBSCRIPTION_REMINDER_SCHEDULE_HOURS:
        if hours_left <= float(threshold):
            return int(threshold)
    return None


async def subscription_expiry_reminder_loop() -> None:
    interval = max(60, SUBSCRIPTION_REMINDER_INTERVAL_SECONDS)
    while True:
        try:
            if SUBSCRIPTION_REMINDER_ENABLED:
                await send_subscription_expiry_reminders()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[sub-reminder] Loop error: {exc}")

        await asyncio.sleep(interval)


async def send_subscription_expiry_reminders() -> None:
    max_window = max(SUBSCRIPTION_REMINDER_SCHEDULE_HOURS or [SUBSCRIPTION_REMINDER_HOURS])
    users = get_users_with_expiring_subscription(max_window)
    if not users:
        return

    now_at = dt.datetime.now()
    sent = 0
    for user in users:
        telegram_id = int(user["telegram_id"])
        subscription_end = str(user["subscription_end"] or "").strip()
        if not subscription_end:
            continue

        expiry = parse_date(subscription_end)
        if not expiry:
            continue
        hours_left = (expiry - now_at).total_seconds() / 3600.0
        threshold_hours = pick_subscription_reminder_threshold(hours_left)
        if threshold_hours is None:
            continue

        reminder_key = build_subscription_expiry_reminder_key(telegram_id, threshold_hours)
        if get_app_meta(reminder_key) == subscription_end:
            continue

        remaining = format_subscription_remaining(subscription_end)
        try:
            text = (
                "⏰ Напоминание: подписка скоро закончится.\n"
                f"До окончания ~{threshold_hours} ч.\n"
                f"До: {subscription_end}\n"
                f"Осталось: {remaining}\n\n"
                "Рекомендуем продлить заранее, чтобы не потерять доступ."
            )
            await bot.send_message(
                telegram_id,
                text,
                reply_markup=build_subscription_reminder_keyboard(),
            )
            set_app_meta(reminder_key, subscription_end)
            sent += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[sub-reminder] Failed to notify {telegram_id}: {exc}")

    if sent > 0:
        print(
            f"[sub-reminder] Sent {sent} reminder(s) "
            f"within {max_window}h window"
        )


def support_sla_alert_meta_key(ticket_id: int) -> str:
    return f"support_sla_alert:{int(ticket_id)}"


def get_support_sla_overdue_tickets(
    sla_minutes: int,
    limit: int = 20,
) -> list[sqlite3.Row]:
    cutoff = (dt.datetime.now() - dt.timedelta(minutes=max(1, int(sla_minutes)))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn = get_conn()
    rows = conn.execute(
        """
        WITH user_last AS (
            SELECT ticket_id, MAX(id) AS msg_id
            FROM support_ticket_messages
            WHERE sender_role = 'user'
            GROUP BY ticket_id
        ),
        admin_last AS (
            SELECT ticket_id, MAX(id) AS msg_id
            FROM support_ticket_messages
            WHERE sender_role = 'admin'
            GROUP BY ticket_id
        )
        SELECT
            t.id,
            t.telegram_id,
            t.username,
            t.status,
            t.assigned_admin_id,
            um.id AS user_msg_id,
            um.created_at AS user_msg_at,
            um.message AS user_message,
            um.media_kind AS user_media_kind,
            am.id AS admin_msg_id,
            am.created_at AS admin_msg_at
        FROM support_tickets t
        JOIN user_last ul ON ul.ticket_id = t.id
        JOIN support_ticket_messages um ON um.id = ul.msg_id
        LEFT JOIN admin_last al ON al.ticket_id = t.id
        LEFT JOIN support_ticket_messages am ON am.id = al.msg_id
        WHERE t.status != 'closed'
          AND um.created_at <= ?
          AND (am.id IS NULL OR am.id < um.id)
        ORDER BY um.created_at ASC
        LIMIT ?
        """,
        (cutoff, max(1, min(int(limit), 500))),
    ).fetchall()
    conn.close()
    return rows


async def send_support_sla_alerts() -> None:
    if not SUPPORT_SLA_ENABLED:
        return
    if not get_admin_notification_targets():
        return

    rows = get_support_sla_overdue_tickets(
        sla_minutes=SUPPORT_SLA_MINUTES,
        limit=SUPPORT_SLA_ALERT_LIMIT,
    )
    if not rows:
        return

    sent = 0
    for row in rows:
        ticket_id = int(row["id"])
        user_msg_at = str(row["user_msg_at"] or "")
        if not user_msg_at:
            continue
        key = support_sla_alert_meta_key(ticket_id)
        if get_app_meta(key) == user_msg_at:
            continue

        user_label = f"@{str(row['username']).strip()}" if str(row["username"] or "").strip() else str(
            int(row["telegram_id"] or 0)
        )
        assignee = (
            admin_label(int(row["assigned_admin_id"]))
            if row["assigned_admin_id"] is not None
            else "не назначен"
        )
        payload = str(row["user_message"] or "").strip() or "Без текста."
        payload = " ".join(payload.split())
        if len(payload) > 180:
            payload = f"{payload[:179]}…"

        text = (
            "⏱ SLA-алерт по поддержке\n"
            f"Тикет: #{ticket_id}\n"
            f"Пользователь: {user_label}\n"
            f"Последнее сообщение: {user_msg_at}\n"
            f"Назначен: {assignee}\n"
            f"Таймер SLA: > {SUPPORT_SLA_MINUTES} мин\n"
            f"Сообщение: {payload}"
        )
        sent_count, _ = await send_admin_notification_text(text)
        if sent_count > 0:
            set_app_meta(key, user_msg_at)
            sent += 1

    if sent > 0:
        print(f"[support-sla] Sent {sent} alert(s)")


async def support_sla_loop() -> None:
    interval = max(60, SUPPORT_SLA_CHECK_INTERVAL_SECONDS)
    while True:
        try:
            if SUPPORT_SLA_ENABLED:
                await send_support_sla_alerts()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[support-sla] Loop error: {exc}")
        await asyncio.sleep(interval)


def auto_backup_last_attempt_key() -> str:
    return "auto_backup_last_attempt"


def auto_backup_last_success_key() -> str:
    return "auto_backup_last_success"


def resolve_backup_target_paths() -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for raw_target in AUTO_BACKUP_TARGETS:
        target = Path(str(raw_target).strip())
        if not target.is_absolute():
            target = BASE_DIR / target

        candidates: list[Path] = [target]
        if target.resolve() == Path(DB_PATH).resolve():
            candidates.extend([Path(f"{target}-wal"), Path(f"{target}-shm")])

        for item in candidates:
            key = str(item)
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
    return result


def prune_backup_archives(backup_dir: Path, keep_files: int) -> int:
    keep = max(1, int(keep_files))
    archives = sorted(
        [path for path in backup_dir.glob("boxvolt_backup_*.tar.gz") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    deleted = 0
    for stale in archives[keep:]:
        try:
            stale.unlink(missing_ok=True)
            deleted += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[backup] Failed to remove old backup {stale}: {exc}")
    return deleted


def run_auto_backup() -> tuple[bool, str]:
    backup_dir = Path(AUTO_BACKUP_DIR or str(BASE_DIR / "backups")).expanduser()
    backup_dir.mkdir(parents=True, exist_ok=True)

    now_at = dt.datetime.now()
    archive_name = f"boxvolt_backup_{now_at.strftime('%Y%m%d_%H%M%S')}.tar.gz"
    archive_path = backup_dir / archive_name
    targets = resolve_backup_target_paths()

    added_count = 0
    missing: list[str] = []
    with tarfile.open(archive_path, "w:gz") as tar:
        for target in targets:
            if target.exists() and target.is_file():
                tar.add(str(target), arcname=target.name)
                added_count += 1
            else:
                missing.append(str(target))

    if added_count <= 0:
        with contextlib.suppress(Exception):
            archive_path.unlink(missing_ok=True)
        return False, "no_backup_targets_found"

    deleted = prune_backup_archives(backup_dir, AUTO_BACKUP_KEEP_FILES)
    tail = f", missing={len(missing)}" if missing else ""
    prune_tail = f", pruned={deleted}" if deleted > 0 else ""
    return True, f"{archive_path.name} (files={added_count}{tail}{prune_tail})"


async def maybe_run_auto_backup(force: bool = False) -> tuple[bool, str]:
    if not AUTO_BACKUP_ENABLED and not force:
        return False, "disabled"

    now_at = dt.datetime.now()
    if not force:
        last_attempt = parse_date(get_app_meta(auto_backup_last_attempt_key()))
        if last_attempt and (now_at - last_attempt).total_seconds() < AUTO_BACKUP_INTERVAL_SECONDS:
            return False, "too_early"

    set_app_meta(auto_backup_last_attempt_key(), now_str())
    ok, info = run_auto_backup()
    if ok:
        set_app_meta(auto_backup_last_success_key(), now_str())
        print(f"[backup] Created: {info}")
        return True, info

    error_text = f"⚠️ Ошибка auto-backup: {info}"
    print(f"[backup] {error_text}")
    if get_admin_notification_targets():
        try:
            await send_admin_notification_text(error_text)
        except Exception as exc:  # noqa: BLE001
            print(f"[backup] Failed to notify admins: {exc}")
    return False, info


async def auto_backup_loop() -> None:
    interval = max(60, AUTO_BACKUP_CHECK_INTERVAL_SECONDS)
    while True:
        try:
            if AUTO_BACKUP_ENABLED:
                await maybe_run_auto_backup(force=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[backup] Loop error: {exc}")
        await asyncio.sleep(interval)


def service_monitor_meta_key(service: str) -> str:
    normalized = re.sub(r"[^a-z0-9_.-]+", "_", str(service or "").strip().lower()) or "unknown"
    return f"service_monitor:{normalized}"


async def systemd_service_state(service: str) -> str:
    try:
        proc = await asyncio.create_subprocess_exec(
            "systemctl",
            "is-active",
            str(service),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        raw = (stdout or stderr or b"").decode("utf-8", errors="ignore").strip().lower()
        if raw:
            return raw
        if proc.returncode == 0:
            return "active"
        return f"unknown({proc.returncode})"
    except FileNotFoundError:
        return "systemctl_not_found"
    except asyncio.TimeoutError:
        return "timeout"
    except Exception as exc:  # noqa: BLE001
        return f"error:{exc}"


async def check_services_and_notify() -> None:
    if not SERVICE_MONITOR_ENABLED or not SERVICE_MONITOR_SERVICES:
        return
    if not get_admin_notification_targets():
        return

    for service in SERVICE_MONITOR_SERVICES:
        current_state = await systemd_service_state(service)
        is_up = current_state == "active"
        meta_key = service_monitor_meta_key(service)
        previous_state = str(get_app_meta(meta_key) or "").strip().lower()

        if is_up:
            if previous_state and previous_state != "active" and SERVICE_MONITOR_NOTIFY_RECOVERY:
                await send_admin_notification_text(
                    f"✅ Сервис восстановлен: {service}\nТекущее состояние: {current_state}"
                )
            set_app_meta(meta_key, "active")
            continue

        if previous_state != current_state:
            await send_admin_notification_text(
                f"🚨 Сервис недоступен: {service}\nСостояние: {current_state}"
            )
        set_app_meta(meta_key, current_state)


async def service_monitor_loop() -> None:
    interval = max(30, SERVICE_MONITOR_INTERVAL_SECONDS)
    while True:
        try:
            if SERVICE_MONITOR_ENABLED:
                await check_services_and_notify()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[service-monitor] Loop error: {exc}")
        await asyncio.sleep(interval)


def serialize_plan(plan: Plan) -> dict[str, Any]:
    return {
        "code": plan.code,
        "title": plan.title,
        "days": plan.days,
        "amount_rub": plan.amount_rub,
        "base_amount_rub": plan.base_amount_rub or plan.amount_rub,
        "discount_percent": plan.discount_percent,
    }


def validate_webapp_init_data(init_data: str) -> tuple[bool, dict[str, Any] | None, str]:
    if not init_data:
        return False, None, "missing_init_data"

    raw_data = dict(parse_qsl(init_data, keep_blank_values=True))
    incoming_hash = raw_data.pop("hash", "")
    if not incoming_hash:
        return False, None, "missing_hash"

    check_string = "\n".join(f"{k}={v}" for k, v in sorted(raw_data.items()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calculated_hash, incoming_hash):
        return False, None, "bad_signature"

    auth_date = raw_data.get("auth_date")
    if auth_date and auth_date.isdigit():
        now_ts = int(dt.datetime.now().timestamp())
        age = now_ts - int(auth_date)
        if age > WEBAPP_INITDATA_MAX_AGE_SECONDS:
            return False, None, "init_data_expired"

    user_raw = raw_data.get("user")
    if not user_raw:
        return False, None, "missing_user"

    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError:
        return False, None, "bad_user_json"

    if not isinstance(user, dict) or not str(user.get("id", "")).isdigit():
        return False, None, "bad_user_payload"

    raw_data["user_obj"] = user
    return True, raw_data, "ok"


def subscription_end_to_ms(subscription_end: str | None) -> int:
    end_at = parse_date(subscription_end)
    if not end_at:
        return 0
    return int(end_at.timestamp() * 1000)


def default_reality_profile() -> dict[str, str]:
    return {
        "public_key": PUBLIC_KEY,
        "short_id": SHORT_ID,
        "sni": SNI,
        "fingerprint": UTLS_FP,
    }


def parse_inbound_clients(inbound_obj: dict[str, Any]) -> list[dict[str, Any]]:
    settings_raw = inbound_obj.get("settings") or "{}"
    try:
        settings = json.loads(settings_raw)
    except (json.JSONDecodeError, TypeError):
        settings = {}

    clients = settings.get("clients")
    if not isinstance(clients, list):
        return []
    return [client for client in clients if isinstance(client, dict)]


def parse_inbound_client_stats(inbound_obj: dict[str, Any]) -> list[dict[str, Any]]:
    raw_stats = inbound_obj.get("clientStats")
    if not isinstance(raw_stats, list):
        return []
    return [item for item in raw_stats if isinstance(item, dict)]


def find_inbound_client(
    clients: list[dict[str, Any]],
    telegram_id: int,
    client_uuid: str | None = None,
) -> dict[str, Any] | None:
    expected_telegram_id = str(telegram_id)
    expected_uuid = str(client_uuid or "").strip()

    if expected_uuid:
        for item in clients:
            candidate = str(item.get("id") or "").strip()
            if candidate == expected_uuid:
                return item

    for item in clients:
        if str(item.get("email") or "").strip() == expected_telegram_id:
            return item
    return None


def find_inbound_client_stat(
    client_stats: list[dict[str, Any]],
    telegram_id: int,
    client_uuid: str | None = None,
) -> dict[str, Any] | None:
    expected_telegram_id = str(telegram_id)
    expected_uuid = str(client_uuid or "").strip()

    if expected_uuid:
        for item in client_stats:
            candidate = str(item.get("uuid") or item.get("id") or "").strip()
            if candidate == expected_uuid:
                return item

    for item in client_stats:
        if str(item.get("email") or "").strip() == expected_telegram_id:
            return item
    return None


def extract_reality_profile_from_inbound(inbound_obj: dict[str, Any]) -> dict[str, str]:
    profile = default_reality_profile()

    stream_raw = inbound_obj.get("streamSettings") or "{}"
    try:
        stream = json.loads(stream_raw)
    except (json.JSONDecodeError, TypeError):
        stream = {}

    if not isinstance(stream, dict):
        return profile

    reality = stream.get("realitySettings")
    if not isinstance(reality, dict):
        return profile

    settings = reality.get("settings")
    if isinstance(settings, dict):
        public_key = str(settings.get("publicKey") or "").strip()
        fingerprint = str(settings.get("fingerprint") or "").strip()
        if public_key:
            profile["public_key"] = public_key
        if fingerprint:
            profile["fingerprint"] = fingerprint

    short_ids = reality.get("shortIds")
    if isinstance(short_ids, list) and short_ids:
        first_short_id = str(short_ids[0] or "").strip()
        if first_short_id:
            profile["short_id"] = first_short_id

    server_names = reality.get("serverNames")
    if isinstance(server_names, list) and server_names:
        first_server_name = str(server_names[0] or "").strip()
        if first_server_name:
            profile["sni"] = first_server_name
    else:
        target = str(reality.get("target") or "").strip()
        target_host = target.split(":", maxsplit=1)[0]
        if target_host:
            profile["sni"] = target_host

    return profile


def cache_reality_profile(profile: dict[str, str]) -> None:
    global REALITY_PROFILE_CACHE, REALITY_PROFILE_CACHE_AT
    REALITY_PROFILE_CACHE = profile
    REALITY_PROFILE_CACHE_AT = dt.datetime.now()


def reality_profile_cache_valid() -> bool:
    if not REALITY_PROFILE_CACHE or not REALITY_PROFILE_CACHE_AT:
        return False
    age = (dt.datetime.now() - REALITY_PROFILE_CACHE_AT).total_seconds()
    return age <= max(5, REALITY_PROFILE_CACHE_SECONDS)


async def xui_login(client: httpx.AsyncClient) -> httpx.Cookies:
    login_resp = await client.post(
        f"{XUI_URL}/login",
        data={"username": XUI_USERNAME, "password": XUI_PASSWORD},
    )
    login_resp.raise_for_status()
    return login_resp.cookies


async def xui_get_inbound_by_id(
    client: httpx.AsyncClient,
    cookies: httpx.Cookies,
    inbound_id: int,
) -> dict[str, Any]:
    resp = await client.get(f"{XUI_URL}/panel/api/inbounds/get/{inbound_id}", cookies=cookies)
    resp.raise_for_status()

    try:
        body = resp.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"3x-ui get inbound invalid json: {exc}") from exc

    if not isinstance(body, dict) or not body.get("success"):
        raise RuntimeError(f"3x-ui get inbound failed: {body}")

    obj = body.get("obj")
    if not isinstance(obj, dict):
        raise RuntimeError("3x-ui get inbound response missing object")
    return obj


async def xui_get_inbound(client: httpx.AsyncClient, cookies: httpx.Cookies) -> dict[str, Any]:
    return await xui_get_inbound_by_id(client, cookies, INBOUND_ID)


async def xui_get_inbound_from_list(
    client: httpx.AsyncClient,
    cookies: httpx.Cookies,
    inbound_id: int = INBOUND_ID,
) -> dict[str, Any]:
    resp = await client.get(f"{XUI_URL}/panel/api/inbounds/list", cookies=cookies)
    resp.raise_for_status()

    try:
        body = resp.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"3x-ui list inbounds invalid json: {exc}") from exc

    if not isinstance(body, dict) or not body.get("success"):
        raise RuntimeError(f"3x-ui list inbounds failed: {body}")

    obj = body.get("obj")
    if not isinstance(obj, list):
        raise RuntimeError("3x-ui list inbounds response missing list object")

    inbound_id_text = str(inbound_id)
    for inbound in obj:
        if not isinstance(inbound, dict):
            continue
        if str(inbound.get("id") or "").strip() == inbound_id_text:
            return inbound

    raise RuntimeError(f"3x-ui inbound id {inbound_id} not found in list response")


async def xui_get_client_traffic_bytes(
    telegram_id: int,
    client_uuid: str | None = None,
) -> tuple[int, int, int]:
    if not all([XUI_URL, XUI_USERNAME, XUI_PASSWORD]):
        return 0, 0, subscription_total_bytes()

    async with httpx.AsyncClient(timeout=20.0) as client:
        cookies = await xui_login(client)
        inbound_obj = await xui_get_inbound_from_list(client, cookies)

    client_stats = parse_inbound_client_stats(inbound_obj)
    stat_item = find_inbound_client_stat(client_stats, telegram_id, client_uuid)
    upload_bytes = non_negative_int(stat_item.get("up")) if stat_item else 0
    download_bytes = non_negative_int(stat_item.get("down")) if stat_item else 0
    total_bytes = non_negative_int(stat_item.get("total")) if stat_item else 0

    if total_bytes <= 0:
        inbound_clients = parse_inbound_clients(inbound_obj)
        client_item = find_inbound_client(inbound_clients, telegram_id, client_uuid)
        if client_item:
            total_bytes = non_negative_int(client_item.get("totalGB"))

    if total_bytes <= 0:
        total_bytes = subscription_total_bytes()

    return upload_bytes, download_bytes, total_bytes


async def xui_delete_client(
    client: httpx.AsyncClient,
    cookies: httpx.Cookies,
    client_uuid: str,
) -> None:
    resp = await client.post(
        f"{XUI_URL}/panel/api/inbounds/{INBOUND_ID}/delClient/{client_uuid}",
        cookies=cookies,
    )
    resp.raise_for_status()
    try:
        body = resp.json()
    except json.JSONDecodeError:
        return
    if isinstance(body, dict) and body.get("success") is False:
        raise RuntimeError(f"3x-ui del client failed: {body}")


def build_xui_client_payload(
    telegram_id: int,
    client_uuid: str,
    expiry_time_ms: int,
    existing_client: dict[str, Any] | None = None,
    fallback_flow: str = "",
    flow_override: str | None = None,
    email_value: str | None = None,
) -> dict[str, Any]:
    payload = dict(existing_client or {})
    payload["id"] = client_uuid
    payload["email"] = str(email_value if email_value is not None else telegram_id)
    if flow_override is not None:
        payload["flow"] = str(flow_override).strip()
    elif XUI_FLOW != "":
        payload["flow"] = XUI_FLOW
    else:
        payload["flow"] = str(payload.get("flow") or fallback_flow)
    payload["limitIp"] = max(0, XUI_LIMIT_IP)
    payload["totalGB"] = gigabytes_to_bytes(XUI_TOTAL_GB)
    payload["expiryTime"] = max(0, expiry_time_ms)
    payload["enable"] = True

    if not payload.get("subId"):
        payload["subId"] = uuid.uuid4().hex[:16]
    return payload


async def xui_upsert_client_for_inbound(
    inbound_id: int,
    telegram_id: int,
    preferred_uuid: str | None,
    subscription_end: str | None,
    *,
    flow_override: str | None = None,
    cache_main_reality: bool = False,
    email_override: str | None = None,
) -> tuple[str, dict[str, Any]]:
    if not all([XUI_URL, XUI_USERNAME, XUI_PASSWORD]):
        raise RuntimeError("3x-ui config is incomplete in .env")

    expiry_time_ms = subscription_end_to_ms(subscription_end)
    client_email = str(email_override).strip() if email_override else str(telegram_id)

    async with httpx.AsyncClient(timeout=20.0) as client:
        cookies = await xui_login(client)
        inbound_obj = await xui_get_inbound_by_id(client, cookies, inbound_id)
        if cache_main_reality:
            cache_reality_profile(extract_reality_profile_from_inbound(inbound_obj))

        clients = parse_inbound_clients(inbound_obj)
        fallback_flow = ""
        for item in clients:
            flow_value = str(item.get("flow") or "").strip()
            if flow_value:
                fallback_flow = flow_value
                break

        found_client: dict[str, Any] | None = None

        if preferred_uuid:
            for item in clients:
                if str(item.get("id") or "") == preferred_uuid:
                    found_client = item
                    break

        if not found_client:
            for item in clients:
                if str(item.get("email") or "") == client_email:
                    found_client = item
                    break

        current_uuid = str(found_client.get("id") or "") if found_client else ""
        client_uuid = preferred_uuid or current_uuid or str(uuid.uuid4())

        payload = build_xui_client_payload(
            telegram_id=telegram_id,
            client_uuid=client_uuid,
            expiry_time_ms=expiry_time_ms,
            existing_client=found_client,
            fallback_flow=fallback_flow,
            flow_override=flow_override,
            email_value=client_email,
        )
        settings_payload = {"clients": [payload]}

        if found_client:
            update_id = current_uuid or client_uuid
            resp = await client.post(
                f"{XUI_URL}/panel/api/inbounds/updateClient/{update_id}",
                data={"id": inbound_id, "settings": json.dumps(settings_payload, ensure_ascii=False)},
                cookies=cookies,
            )
        else:
            resp = await client.post(
                f"{XUI_URL}/panel/api/inbounds/addClient",
                data={"id": inbound_id, "settings": json.dumps(settings_payload, ensure_ascii=False)},
                cookies=cookies,
            )
        resp.raise_for_status()

        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"3x-ui upsert client invalid json: {exc}") from exc

        if not isinstance(body, dict) or body.get("success") is False:
            raise RuntimeError(f"3x-ui upsert client failed: {body}")

        return client_uuid, inbound_obj


async def xui_upsert_client(
    telegram_id: int,
    preferred_uuid: str | None,
    subscription_end: str | None,
) -> str:
    client_uuid, _ = await xui_upsert_client_for_inbound(
        inbound_id=INBOUND_ID,
        telegram_id=telegram_id,
        preferred_uuid=preferred_uuid,
        subscription_end=subscription_end,
        flow_override=None,
        cache_main_reality=True,
    )
    return client_uuid


async def get_reality_profile(force_refresh: bool = False) -> dict[str, str]:
    if not force_refresh and reality_profile_cache_valid():
        return REALITY_PROFILE_CACHE or default_reality_profile()

    if not all([XUI_URL, XUI_USERNAME, XUI_PASSWORD]):
        return default_reality_profile()

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            cookies = await xui_login(client)
            inbound_obj = await xui_get_inbound(client, cookies)
    except Exception as exc:  # noqa: BLE001
        print(f"[xui] Failed to refresh reality profile: {exc}")
        return REALITY_PROFILE_CACHE or default_reality_profile()

    profile = extract_reality_profile_from_inbound(inbound_obj)
    cache_reality_profile(profile)
    return profile


async def generate_vless_link(user_uuid: str) -> str:
    return await generate_vless_link_with_options(user_uuid=user_uuid)


def speed_profile_display_name() -> str:
    name = str(SPEED_PROFILE_NAME or "").strip()
    return name or "Скоростной"


def reserve_profile_display_name() -> str:
    return str(ROUTE_RESERVE_NAME or "").strip() or "Резервный"


def speed_inbound_email(telegram_id: int) -> str:
    return f"{telegram_id}-speed"


def reserve_inbound_email(telegram_id: int) -> str:
    return f"{telegram_id}-reserve"


def youtube_profile_display_name() -> str:
    name = str(YOUTUBE_PROFILE_NAME or "").strip()
    return name or "Ютуб без рекламы"


def youtube_inbound_email(telegram_id: int) -> str:
    return f"{telegram_id}-yt"


async def generate_vless_link_with_options(
    user_uuid: str,
    *,
    display_name: str | None = None,
    flow_override: str | None = None,
    reality_profile: dict[str, str] | None = None,
    server_port: int | None = None,
) -> str:
    profile = reality_profile or await get_reality_profile()
    params = {
        "encryption": "none",
        "type": "tcp",
        "security": "reality",
        "sni": profile.get("sni") or SNI,
        "fp": profile.get("fingerprint") or UTLS_FP,
        "pbk": profile.get("public_key") or PUBLIC_KEY,
        "sid": profile.get("short_id") or SHORT_ID,
    }
    flow_value = XUI_FLOW if flow_override is None else str(flow_override or "").strip()
    if flow_value:
        params["flow"] = flow_value
    query = urlencode(params)
    node_name = str(display_name or "").strip() or build_vless_display_name()
    resolved_port = server_port if isinstance(server_port, int) and server_port > 0 else SERVER_PORT
    return f"vless://{user_uuid}@{SERVER_IP}:{resolved_port}?{query}#{quote(node_name, safe='')}"


def build_vless_display_name() -> str:
    prefix = str(SERVER_NODE_PREFIX or "").strip()
    flag = str(SERVER_FLAG_EMOJI or "").strip()
    country = str(SERVER_COUNTRY or "").strip()

    if flag and country and country.startswith(flag):
        country_block = country
    else:
        country_block = " ".join(part for part in (flag, country) if part)
    if prefix and country_block:
        return f"{prefix} — {country_block} ⚡⚡"
    if prefix:
        return prefix
    if country_block:
        return country_block
    return "BoxVoltVPN"


def server_country_label() -> str:
    flag = str(SERVER_FLAG_EMOJI or "").strip()
    country = str(SERVER_COUNTRY or "").strip()
    if flag and country and country.startswith(flag):
        return country
    return " ".join(part for part in (flag, country) if part) or "Global"


def as_copyable_key(link: str) -> str:
    return f"<code>{html.escape(link)}</code>"


async def notify_admins_new_ticket(ticket_id: int) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return
    if not ADMIN_TELEGRAM_IDS:
        return

    text = support_ticket_admin_text(ticket)
    keyboard = build_ticket_admin_keyboard(ticket_id)
    message_ids = SUPPORT_TICKET_ADMIN_MESSAGE_IDS.setdefault(ticket_id, {})

    for admin_id in ADMIN_TELEGRAM_IDS:
        try:
            sent = await bot.send_message(admin_id, text, reply_markup=keyboard)
            message_ids[admin_id] = sent.message_id
        except Exception as exc:  # noqa: BLE001
            print(f"[ticket] Failed to notify admin {admin_id} about ticket {ticket_id}: {exc}")


async def refresh_ticket_for_admins(
    ticket_id: int,
    assigned_label: str | None = None,
    *,
    only_assigned: bool = False,
) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return
    if not ADMIN_TELEGRAM_IDS:
        return

    text = support_ticket_admin_text(ticket, assigned_label=assigned_label)
    keyboard = build_ticket_admin_keyboard(ticket_id)
    message_ids = SUPPORT_TICKET_ADMIN_MESSAGE_IDS.setdefault(ticket_id, {})
    target_admin_ids = list(ADMIN_TELEGRAM_IDS)
    assigned_admin_id = ticket["assigned_admin_id"]
    if only_assigned and assigned_admin_id:
        target_admin_ids = [int(assigned_admin_id)]

    for admin_id in target_admin_ids:
        message_id = message_ids.get(admin_id)
        if message_id:
            try:
                await bot.edit_message_text(
                    chat_id=admin_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=keyboard,
                )
                continue
            except TelegramBadRequest as exc:
                if "message is not modified" in str(exc).lower():
                    continue
            except Exception:
                pass
        try:
            sent = await bot.send_message(admin_id, text, reply_markup=keyboard)
            message_ids[admin_id] = sent.message_id
        except Exception as exc:  # noqa: BLE001
            print(f"[ticket] Failed to refresh admin {admin_id} for ticket {ticket_id}: {exc}")


async def notify_admins_ticket_taken(ticket_id: int, taken_by_admin_id: int, taken_label: str) -> None:
    if not ADMIN_TELEGRAM_IDS:
        return
    for admin_id in ADMIN_TELEGRAM_IDS:
        if admin_id == taken_by_admin_id:
            continue
        try:
            await bot.send_message(
                admin_id,
                f"ℹ️ Тикет #{ticket_id} взял администратор {taken_label}.",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[ticket] Failed to send takeover notice to admin {admin_id}: {exc}")


async def notify_user_ticket_taken(ticket_id: int, admin_id: int) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return
    try:
        await bot.send_message(
            int(ticket["telegram_id"]),
            f"🧑‍💻 Администратор взялся за ваш тикет #{ticket_id}. Скоро отправим ответ.",
            reply_markup=build_support_ticket_keyboard(),
        )
        add_support_ticket_message(
            ticket_id=ticket_id,
            sender_role="system",
            sender_id=admin_id,
            message_text=f"ticket_taken_by:{admin_id}",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ticket] Failed to notify user about taken ticket {ticket_id}: {exc}")


async def notify_admins_user_ticket_message(
    ticket_id: int,
    telegram_id: int,
    username: str | None,
    message_text: str,
    media_kind: str = "text",
    media_file_id: str | None = None,
) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return
    await refresh_ticket_for_admins(ticket_id, only_assigned=bool(ticket["assigned_admin_id"]))


async def send_ticket_dialog_history(
    chat_id: int,
    ticket_id: int,
    *,
    header: str,
    limit: int = 40,
) -> None:
    rows = get_support_ticket_messages(ticket_id=ticket_id, limit=limit, newest_first=False)
    if not rows:
        await bot.send_message(chat_id, f"{header}\nИстория пуста.")
        return
    await bot.send_message(chat_id, f"{header}\nПоказаны последние {len(rows)} сообщений.")
    for row in rows:
        actor = support_ticket_message_actor(str(row["sender_role"] or ""), row["sender_id"])
        text_body = support_ticket_message_text(row)
        created_at = str(row["created_at"] or "-")
        caption = f"{actor} • {created_at}\n\n{text_body}"
        media_kind = str(row["media_kind"] or "text").lower()
        media_file_id = str(row["media_file_id"] or "").strip()
        if media_kind == "photo" and media_file_id:
            await bot.send_photo(chat_id, media_file_id, caption=caption[:1024])
            continue
        if media_kind == "document" and media_file_id:
            await bot.send_document(chat_id, media_file_id, caption=caption[:1024])
            continue
        await bot.send_message(chat_id, caption)


async def deliver_admin_message_to_user(
    ticket_id: int,
    *,
    message_text: str,
    media_kind: str = "text",
    media_file_id: str | None = None,
) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        raise RuntimeError("ticket_not_found")

    user_id = int(ticket["telegram_id"])
    user_in_chat = USER_ACTIVE_TICKET_CHAT_BY_USER.get(user_id) == ticket_id and user_id not in USER_TICKET_CHAT_DISABLED
    if not user_in_chat:
        await bot.send_message(
            user_id,
            f"🔔 Вам пришло сообщение по тикету #{ticket_id}.\n"
            "Откройте «🛟 Поддержка», чтобы снова войти в чат.",
            reply_markup=build_main_keyboard(user_id),
        )

    if media_kind == "photo" and media_file_id:
        caption = (
            f"💬 Ответ администратора по тикету #{ticket_id}\n\n{message_text}"
            if message_text
            else f"💬 Ответ администратора по тикету #{ticket_id}"
        )
        await bot.send_photo(
            user_id,
            media_file_id,
            caption=caption[:1024],
            reply_markup=build_support_ticket_keyboard() if user_in_chat else None,
        )
        return
    if media_kind == "document" and media_file_id:
        caption = (
            f"💬 Ответ администратора по тикету #{ticket_id}\n\n{message_text}"
            if message_text
            else f"💬 Ответ администратора по тикету #{ticket_id}"
        )
        await bot.send_document(
            user_id,
            media_file_id,
            caption=caption[:1024],
            reply_markup=build_support_ticket_keyboard() if user_in_chat else None,
        )
        return

    await bot.send_message(
        user_id,
        f"💬 Ответ администратора по тикету #{ticket_id}:\n{message_text}",
        reply_markup=build_support_ticket_keyboard() if user_in_chat else None,
    )


def build_user_ticket_forward_caption(ticket_id: int, message_text: str) -> str:
    payload = (message_text or "Без текста.").strip()
    return f"👤 Тикет #{ticket_id} • сообщение пользователя\n{payload}".strip()


async def flush_support_user_media_group_buffer(key: tuple[int, int, str]) -> None:
    await asyncio.sleep(0.8)
    bucket = SUPPORT_USER_MEDIA_GROUP_BUFFER.pop(key, None)
    if not bucket:
        return

    admin_id, ticket_id, _ = key
    if ADMIN_REPLY_TICKET_BY_ADMIN.get(admin_id) != ticket_id:
        return
    items: list[dict[str, str]] = list(bucket.get("items") or [])
    if not items:
        return

    first_caption = build_user_ticket_forward_caption(ticket_id, items[0].get("message_text", ""))
    if len(items) == 1:
        kind = items[0].get("media_kind", "text")
        file_id = items[0].get("media_file_id", "")
        if kind == "photo" and file_id:
            await bot.send_photo(admin_id, file_id, caption=first_caption[:1024])
            return
        if kind == "document" and file_id:
            await bot.send_document(admin_id, file_id, caption=first_caption[:1024])
            return
        await bot.send_message(admin_id, first_caption)
        return

    all_photo = all(item.get("media_kind") == "photo" and item.get("media_file_id") for item in items)
    all_document = all(item.get("media_kind") == "document" and item.get("media_file_id") for item in items)
    if all_photo:
        media_payload = []
        for idx, item in enumerate(items):
            media_payload.append(
                InputMediaPhoto(
                    media=str(item["media_file_id"]),
                    caption=first_caption[:1024] if idx == 0 else None,
                )
            )
        await bot.send_media_group(admin_id, media=media_payload)
        return
    if all_document:
        media_payload = []
        for idx, item in enumerate(items):
            media_payload.append(
                InputMediaDocument(
                    media=str(item["media_file_id"]),
                    caption=first_caption[:1024] if idx == 0 else None,
                )
            )
        await bot.send_media_group(admin_id, media=media_payload)
        return

    await bot.send_message(admin_id, f"👤 Тикет #{ticket_id} • пользователь отправил несколько файлов.")
    for item in items:
        kind = item.get("media_kind", "text")
        file_id = item.get("media_file_id", "")
        caption = build_user_ticket_forward_caption(ticket_id, item.get("message_text", ""))
        if kind == "photo" and file_id:
            await bot.send_photo(admin_id, file_id, caption=caption[:1024])
            continue
        if kind == "document" and file_id:
            await bot.send_document(admin_id, file_id, caption=caption[:1024])
            continue
        await bot.send_message(admin_id, caption)


async def forward_user_ticket_message_to_admin_if_joined(
    ticket_id: int,
    message_text: str,
    *,
    media_kind: str = "text",
    media_file_id: str | None = None,
    media_group_id: str | None = None,
) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return
    assigned_admin_id = ticket["assigned_admin_id"]
    if not assigned_admin_id:
        return
    admin_id = int(assigned_admin_id)
    if ADMIN_REPLY_TICKET_BY_ADMIN.get(admin_id) != ticket_id:
        return

    if media_kind in {"photo", "document"} and media_file_id:
        group_key = media_group_id if media_group_id else "_nogroup"
        key = (admin_id, ticket_id, group_key)
        bucket = SUPPORT_USER_MEDIA_GROUP_BUFFER.setdefault(key, {"items": [], "task": None})
        bucket["items"].append(
            {
                "media_kind": media_kind,
                "media_file_id": media_file_id,
                "message_text": message_text or "",
            }
        )
        if not bucket.get("task"):
            bucket["task"] = asyncio.create_task(flush_support_user_media_group_buffer(key))
        return

    title = f"👤 Тикет #{ticket_id} • сообщение пользователя"
    payload = (message_text or "Без текста.").strip()
    if media_kind == "photo" and media_file_id:
        caption = f"{title}\n{payload}" if payload else title
        await bot.send_photo(admin_id, media_file_id, caption=caption[:1024])
        return
    if media_kind == "document" and media_file_id:
        caption = f"{title}\n{payload}" if payload else title
        await bot.send_document(admin_id, media_file_id, caption=caption[:1024])
        return
    await bot.send_message(admin_id, f"{title}\n{payload}")


def user_title(message: Message) -> str:
    username = message.from_user.username
    if username:
        return f"@{username}"
    return str(message.from_user.id)


def format_rub_value(value: float) -> str:
    amount = float(value)
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def user_label_from_row(user: sqlite3.Row | None, telegram_id: int) -> str:
    if user:
        username = str(user["username"] or "").strip()
        if username:
            return f"@{username}"
    return str(telegram_id)


async def build_referral_program_message(telegram_id: int) -> tuple[str, InlineKeyboardMarkup]:
    referral_link = await build_referral_link(telegram_id)
    stats = get_referral_stats(telegram_id)
    breakdown_rows = get_referral_link_breakdown(telegram_id, limit=3)
    breakdown_lines: list[str] = []
    for item in breakdown_rows:
        label = str(item["label"] or "").strip()
        code = str(item["code"] or "").strip()
        invited_count = int(item["invited_count"] or 0)
        paid_count = int(item["paid_count"] or 0)
        breakdown_lines.append(
            f"• {label} ({code}): приглашено {invited_count}, оплат {paid_count}"
        )
    breakdown_block = "\n".join(breakdown_lines) if breakdown_lines else "• Пока нет данных"
    text = (
        "👥 Реферальная программа BoxVolt\n\n"
        f"Ваша ссылка:\n{referral_link}\n\n"
        "Условия:\n"
        f"• Приглашенный оплачивает тариф от {REFERRAL_MIN_PLAN_DAYS} дней (сейчас: 30/90/365).\n"
        f"• Вы получаете +{REFERRAL_REWARD_DAYS} {day_word(REFERRAL_REWARD_DAYS)} за каждую такую оплату.\n"
        "• Бонусы суммируются автоматически.\n\n"
        "Статистика:\n"
        f"• Ссылок: {stats['links_count']}\n"
        f"• Приглашено: {stats['invited_count']}\n"
        f"• Оплат по реферальной ссылке: {stats['reward_events']}\n"
        f"• Начислено дней: {stats['reward_days_total']}\n\n"
        "По ссылкам:\n"
        f"{breakdown_block}\n\n"
        f"💡 Бонус начисляется только после успешной оплаты приглашенным пользователем тарифа от {REFERRAL_MIN_PLAN_DAYS} дней."
    )
    return text, build_referral_keyboard(referral_link)


async def notify_admins_paid_order(
    payment: sqlite3.Row,
    user: sqlite3.Row | None,
    new_end: str,
) -> None:
    if not get_admin_notification_targets():
        return

    telegram_id = int(payment["telegram_id"])
    username = html.escape(user_label_from_row(user, telegram_id))
    order_id = html.escape(str(payment["order_id"]))
    days = int(payment["days"] or 0)
    amount = format_rub_value(float(payment["amount_rub"] or 0))
    provider = html.escape(payment_provider_label(str(payment["provider"] or "")))
    plan_code = html.escape(str(payment["plan_code"] or "-"))

    text = (
        "💰 Новая оплата подписки\n"
        f"Пользователь: {username}\n"
        f"Telegram ID: <code>{telegram_id}</code>\n"
        f"Продление: <b>+{days} дн.</b>\n"
        f"Сумма: <b>{amount} ₽</b>\n"
        f"Тариф: <code>{plan_code}</code>\n"
        f"Провайдер: {provider}\n"
        f"Заказ: <code>{order_id}</code>\n"
        f"Подписка до: {html.escape(new_end)}"
    )
    _, failed = await send_admin_notification_text(text, parse_mode="HTML")
    if failed > 0:
        print(f"[admin-notify] Failed to send {failed} message(s)")


async def notify_referrer_reward(reward: dict[str, Any]) -> None:
    referrer_telegram_id = int(reward.get("referrer_telegram_id") or 0)
    if referrer_telegram_id <= 0:
        return

    invited_telegram_id = int(reward.get("invited_telegram_id") or 0)
    invited_username = str(reward.get("invited_username") or "").strip()
    invited_label = f"@{invited_username}" if invited_username else str(invited_telegram_id)
    paid_days = int(reward.get("paid_days") or 0)
    reward_days = int(reward.get("reward_days") or 0)
    new_end = str(reward.get("referrer_new_end") or "-")

    await bot.send_message(
        referrer_telegram_id,
        "🎉 Реферальный бонус зачислен!\n"
        f"Пользователь: {invited_label}\n"
        f"Его покупка: {paid_days} {day_word(paid_days)}\n"
        f"Ваш бонус: +{reward_days} {day_word(reward_days)}\n"
        f"Подписка теперь до: {new_end}",
        reply_markup=build_main_keyboard(referrer_telegram_id),
    )


async def notify_loyalty_reward(reward: dict[str, Any]) -> None:
    telegram_id = int(reward.get("telegram_id") or 0)
    if telegram_id <= 0:
        return
    paid_count = int(reward.get("paid_count") or 0)
    reward_days = int(reward.get("reward_days") or 0)
    new_end = str(reward.get("new_end") or "-")
    await bot.send_message(
        telegram_id,
        "🎁 Лояльность BoxVolt\n"
        f"Спасибо за доверие! Это ваша {paid_count}-я успешная оплата.\n"
        f"Начислено: +{reward_days} {day_word(reward_days)}\n"
        f"Новая дата окончания: {new_end}",
        reply_markup=build_main_keyboard(telegram_id),
    )


async def send_post_payment_onboarding(telegram_id: int) -> None:
    subscription_block = build_subscription_text_block(telegram_id)
    if not subscription_block:
        return
    text = (
        "🧭 Быстрый старт (1 минута)\n"
        "1. Установите приложение для вашего устройства.\n"
        "2. Нажмите «Открыть Subscription URL».\n"
        "3. Импортируйте ссылку и нажмите Connect.\n\n"
        "Если что-то не работает, нажмите «Нужна помощь»."
    )
    await bot.send_message(
        telegram_id,
        text,
        reply_markup=build_onboarding_keyboard(telegram_id),
    )


def update_user_subscription(telegram_id: int, days: int) -> str:
    conn = get_conn()
    row = conn.execute(
        "SELECT subscription_end FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    current_end = parse_date(row["subscription_end"] if row else None)
    base = current_end if current_end and current_end > dt.datetime.now() else dt.datetime.now()
    new_end = base + dt.timedelta(days=days)

    conn.execute(
        "UPDATE users SET subscription_end = ? WHERE telegram_id = ?",
        (new_end.strftime("%Y-%m-%d %H:%M:%S"), telegram_id),
    )
    conn.commit()
    conn.close()
    return new_end.strftime("%Y-%m-%d %H:%M:%S")


def save_user_uuid(telegram_id: int, user_uuid: str) -> None:
    conn = get_conn()
    conn.execute(
        "UPDATE users SET vless_uuid = ? WHERE telegram_id = ?",
        (user_uuid, telegram_id),
    )
    conn.commit()
    conn.close()


def mark_payment_paid(order_id: str, payload: dict[str, Any]) -> None:
    provider_hint = normalize_payment_provider(payload.get("provider")) if isinstance(payload, dict) else ""
    if provider_hint not in {DONATEPAY_PROVIDER, CRYPTOBOT_PROVIDER, LZT_PROVIDER, SECONDARY_PROVIDER}:
        provider_hint = ""
    conn = get_conn()
    if provider_hint:
        conn.execute(
            """
            UPDATE payments
            SET status = 'paid', paid_at = ?, raw_payload = ?, provider = ?
            WHERE order_id = ?
            """,
            (now_str(), json.dumps(payload, ensure_ascii=False), provider_hint, order_id),
        )
    else:
        conn.execute(
            """
            UPDATE payments
            SET status = 'paid', paid_at = ?, raw_payload = ?
            WHERE order_id = ?
            """,
            (now_str(), json.dumps(payload, ensure_ascii=False), order_id),
        )
    conn.commit()
    conn.close()


def _extract_nested(source: Any, *keys: str) -> Any:
    current = source
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_order_id(payload: dict[str, Any]) -> str | None:
    candidates = [
        _extract_nested(payload, "order_id"),
        _extract_nested(payload, "invoice_id"),
        _extract_nested(payload, "payment_id"),
        _extract_nested(payload, "payload"),
        _extract_nested(payload, "metadata", "order_id"),
        _extract_nested(payload, "metadata", "invoice_id"),
        _extract_nested(payload, "metadata", "payment_id"),
        _extract_nested(payload, "data", "order_id"),
        _extract_nested(payload, "data", "invoice_id"),
        _extract_nested(payload, "data", "payment_id"),
        _extract_nested(payload, "data", "payload"),
        _extract_nested(payload, "data", "metadata", "order_id"),
        _extract_nested(payload, "data", "metadata", "invoice_id"),
        _extract_nested(payload, "data", "metadata", "payment_id"),
        _extract_nested(payload, "data", "vars", "order_id"),
        _extract_nested(payload, "data", "vars", "invoice_id"),
        _extract_nested(payload, "data", "vars", "payment_id"),
        _extract_nested(payload, "payload", "order_id"),
        _extract_nested(payload, "payload", "payment_id"),
        _extract_nested(payload, "payload", "payload"),
        _extract_nested(payload, "payload", "invoice_payload"),
    ]

    for value in candidates:
        if value:
            if isinstance(value, (dict, list, tuple, set)):
                continue
            text = str(value).strip()
            if text:
                if text.upper().startswith("BV-"):
                    return text.upper()
                return text

    text_fields = [
        _extract_nested(payload, "message"),
        _extract_nested(payload, "comment"),
        _extract_nested(payload, "additional_data"),
        _extract_nested(payload, "data", "message"),
        _extract_nested(payload, "data", "comment"),
        _extract_nested(payload, "data", "additional_data"),
        _extract_nested(payload, "data", "text"),
        _extract_nested(payload, "data", "vars", "comment"),
        _extract_nested(payload, "data", "vars", "message"),
        _extract_nested(payload, "data", "vars", "text"),
    ]

    for value in text_fields:
        if not value:
            continue
        match = ORDER_ID_RE.search(str(value))
        if match:
            return match.group(0).upper()

    return None


def extract_amount(payload: dict[str, Any]) -> float | None:
    candidates = [
        _extract_nested(payload, "amount"),
        _extract_nested(payload, "sum"),
        _extract_nested(payload, "paid_amount"),
        _extract_nested(payload, "data", "amount"),
        _extract_nested(payload, "data", "amount_main"),
        _extract_nested(payload, "data", "sum"),
        _extract_nested(payload, "data", "paid_amount"),
        _extract_nested(payload, "data", "vars", "amount"),
        _extract_nested(payload, "data", "vars", "sum"),
        _extract_nested(payload, "payload", "amount"),
        _extract_nested(payload, "payload", "paid_amount"),
    ]

    for value in candidates:
        if value in (None, ""):
            continue
        try:
            return float(str(value).replace(",", "."))
        except ValueError:
            continue
    return None


def extract_telegram_id(payload: dict[str, Any]) -> int | None:
    candidates = [
        _extract_nested(payload, "telegram_id"),
        _extract_nested(payload, "metadata", "telegram_id"),
        _extract_nested(payload, "data", "telegram_id"),
        _extract_nested(payload, "data", "metadata", "telegram_id"),
        _extract_nested(payload, "data", "vars", "telegram_id"),
        _extract_nested(payload, "payload", "telegram_id"),
        _extract_nested(payload, "payload", "hidden_message"),
    ]

    for value in candidates:
        if value in (None, ""):
            continue
        text = str(value).strip()
        if text.isdigit():
            return int(text)
    return None


def is_successful_payment(payload: dict[str, Any]) -> bool:
    status = str(_extract_nested(payload, "status") or _extract_nested(payload, "data", "status") or "").lower()
    event = str(_extract_nested(payload, "event") or _extract_nested(payload, "type") or "").lower()

    if any(
        word in status
        for word in (
            "wait",
            "pending",
            "process",
            "created",
            "new",
            "hold",
            "in_progress",
            "not_paid",
            "unpaid",
            "notpaid",
        )
    ):
        return False

    if any(word in status for word in ("fail", "cancel", "reject", "decline")):
        return False

    if status in {"paid", "success", "succeeded", "completed"}:
        return True

    if any(word in event for word in ("donation", "paid", "success")):
        # Donation event without explicit status is treated as success only when status is empty.
        return not status

    # Иногда DA присылает payload без status/event, но с суммой и данными доната.
    return extract_amount(payload) is not None


async def ensure_vless_uuid(
    telegram_id: int,
    existing_uuid: str | None,
    subscription_end: str | None,
) -> str:
    user_uuid = await xui_upsert_client(
        telegram_id=telegram_id,
        preferred_uuid=existing_uuid,
        subscription_end=subscription_end,
    )
    if not existing_uuid or existing_uuid != user_uuid:
        save_user_uuid(telegram_id, user_uuid)
    return user_uuid


async def reissue_vless_uuid(
    telegram_id: int,
    existing_uuid: str | None,
    subscription_end: str | None,
) -> str:
    if not all([XUI_URL, XUI_USERNAME, XUI_PASSWORD]):
        raise RuntimeError("3x-ui config is incomplete in .env")

    new_uuid = str(uuid.uuid4())
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            cookies = await xui_login(client)
            inbound_obj = await xui_get_inbound(client, cookies)
            cache_reality_profile(extract_reality_profile_from_inbound(inbound_obj))

            clients = parse_inbound_clients(inbound_obj)
            delete_targets: list[str] = []

            if existing_uuid:
                delete_targets.append(existing_uuid)

            for item in clients:
                if str(item.get("email") or "") != str(telegram_id):
                    continue
                candidate_uuid = str(item.get("id") or "").strip()
                if candidate_uuid and candidate_uuid not in delete_targets:
                    delete_targets.append(candidate_uuid)

            for client_uuid in delete_targets:
                await xui_delete_client(client, cookies, client_uuid)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"failed to delete old client: {exc}") from exc

    user_uuid = await xui_upsert_client(
        telegram_id=telegram_id,
        preferred_uuid=new_uuid,
        subscription_end=subscription_end,
    )
    save_user_uuid(telegram_id, user_uuid)
    return user_uuid


async def process_paid_order(order_id: str, payload: dict[str, Any]) -> tuple[bool, str]:
    async with PROCESS_LOCK:
        payment = get_payment(order_id)
        if not payment:
            return False, "order_not_found"

        if payment["status"] == "paid":
            return True, "already_paid"
        if payment["status"] != "pending":
            return False, f"not_pending:{payment['status']}"

        incoming_amount = extract_amount(payload)
        expected = float(payment["amount_rub"])

        if incoming_amount is not None and incoming_amount + 0.01 < expected:
            log_suspicious_flag(
                int(payment["telegram_id"] or 0),
                "payment_underpaid_attempt",
                f"order={order_id} incoming={incoming_amount} expected={expected}",
            )
            return False, f"amount_mismatch:{incoming_amount}<{expected}"

        telegram_id = int(payment["telegram_id"])
        new_end = update_user_subscription(telegram_id, int(payment["days"]))
        mark_payment_paid(order_id, payload)
        try:
            consume_promocode_for_paid_order(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[promo] Failed to consume promo for {order_id}: {exc}")
        referral_reward: dict[str, Any] | None = None
        loyalty_reward: dict[str, Any] | None = None
        try:
            referral_reward = apply_referral_reward_for_paid_order(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[referral] Failed to apply reward for {order_id}: {exc}")
        try:
            loyalty_reward = apply_loyalty_reward_for_paid_order(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[loyalty] Failed to apply reward for {order_id}: {exc}")

        user = get_user(telegram_id)
        if not user:
            return False, "user_not_found"

        try:
            await notify_admins_paid_order(payment, user, new_end)
        except Exception as exc:  # noqa: BLE001
            print(f"[admin-notify] Failed to process paid notify for {order_id}: {exc}")

        notify_text = ""
        notify_parse_mode: str | None = None
        if maintenance_mode_enabled():
            notify_text = (
                "✅ Оплата подтверждена, подписка продлена.\n"
                f"📅 Подписка активна до: {new_end}\n\n"
                f"{maintenance_user_block_text()}\n"
                "После завершения работ нажмите «🚀 Подключить VPN»."
            )
        else:
            try:
                await ensure_vless_uuid(
                    telegram_id,
                    user["vless_uuid"],
                    user["subscription_end"],
                )
                subscription_block = build_subscription_text_block(telegram_id)
                if not subscription_block:
                    raise RuntimeError("subscription_url_not_configured")
                notify_parse_mode = "HTML"
                notify_text = (
                    "✅ Оплата подтверждена!\n\n"
                    f"📅 Подписка активна до: {new_end}\n"
                    "🔑 Ваш доступ:\n"
                    f"{subscription_block}\n\n"
                    "📚 Если нужно, откройте раздел «Инструкции» в меню."
                )
            except Exception as exc:  # noqa: BLE001
                notify_text = (
                    "✅ Оплата подтверждена, подписка продлена.\n"
                    "⚠️ Но при выдаче VPN-ключа возникла ошибка.\n"
                    f"Напишите в поддержку: {SUPPORT_CONTACT}\n"
                    f"Техническая ошибка: {exc}"
                )

        try:
            await bot.send_message(
                telegram_id,
                notify_text,
                reply_markup=build_main_keyboard(telegram_id),
                parse_mode=notify_parse_mode,
            )
        except Exception as send_exc:  # noqa: BLE001
            print(f"[notify] Failed to send payment message to {telegram_id}: {send_exc}")

        if referral_reward:
            try:
                await notify_referrer_reward(referral_reward)
            except Exception as exc:  # noqa: BLE001
                print(f"[referral] Failed to notify referrer for {order_id}: {exc}")
        if loyalty_reward:
            try:
                await notify_loyalty_reward(loyalty_reward)
            except Exception as exc:  # noqa: BLE001
                print(f"[loyalty] Failed to notify user for {order_id}: {exc}")
        if not maintenance_mode_enabled():
            try:
                await send_post_payment_onboarding(telegram_id)
            except Exception as exc:  # noqa: BLE001
                print(f"[onboarding] Failed to send for {telegram_id}: {exc}")

    return True, "processed"


async def parse_webhook_payload(request: web.Request) -> dict[str, Any]:
    if request.content_type == "application/json":
        payload = await request.json()
    else:
        data = await request.post()
        payload = {key: value for key, value in data.items()}

    # Часто внешние сервисы шлют JSON строкой в поле data.
    if isinstance(payload.get("data"), str):
        try:
            payload["data"] = json.loads(payload["data"])
        except json.JSONDecodeError:
            pass

    if isinstance(payload.get("metadata"), str):
        try:
            payload["metadata"] = json.loads(payload["metadata"])
        except json.JSONDecodeError:
            pass

    if isinstance(payload.get("vars"), str):
        try:
            payload["vars"] = json.loads(payload["vars"])
        except json.JSONDecodeError:
            pass

    if isinstance(payload.get("payload"), str):
        try:
            payload["payload"] = json.loads(payload["payload"])
        except json.JSONDecodeError:
            pass

    data_block = payload.get("data")
    if isinstance(data_block, dict) and isinstance(data_block.get("vars"), str):
        try:
            data_block["vars"] = json.loads(data_block["vars"])
        except json.JSONDecodeError:
            pass

    return payload


def validate_donatepay_webhook_secret(request: web.Request, payload: dict[str, Any]) -> bool:
    if not DONATEPAY_WEBHOOK_SECRET:
        return True

    auth_header = str(request.headers.get("Authorization") or "").strip()
    bearer_secret = ""
    if auth_header.lower().startswith("bearer "):
        bearer_secret = auth_header[7:].strip()

    incoming = (
        request.headers.get("X-Webhook-Secret")
        or request.headers.get("X-DonatePay-Secret")
        or request.headers.get("X-Donatepay-Secret")
        or request.headers.get("X-DonatePay-Signature")
        or bearer_secret
        or request.query.get("secret")
        or str(payload.get("secret") or "")
    )
    return hmac.compare_digest(str(incoming), DONATEPAY_WEBHOOK_SECRET)


async def donatepay_webhook(request: web.Request) -> web.Response:
    try:
        payload = await parse_webhook_payload(request)
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"ok": False, "error": f"invalid_payload:{exc}"}, status=400)

    if not validate_donatepay_webhook_secret(request, payload):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    wrapped_payload = {
        "provider": DONATEPAY_PROVIDER,
        "event": str(payload.get("event") or payload.get("type") or "donation"),
        "status": payload.get("status") or _extract_nested(payload, "data", "status") or "paid",
        "amount": payload.get("sum") or payload.get("amount") or _extract_nested(payload, "data", "sum"),
        "message": payload.get("message") or payload.get("comment"),
        "comment": payload.get("comment") or payload.get("message"),
        "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        "data": payload,
    }

    if not is_successful_payment(wrapped_payload):
        return web.json_response({"ok": True, "ignored": "not_success_event"})

    order_id, match_reason = resolve_order_id_from_payload(wrapped_payload)
    if order_id and match_reason != "order_id":
        print(f"[dp-webhook] Pending order matched by {match_reason}: {order_id}")

    if not order_id:
        return web.json_response({"ok": False, "error": "order_id_not_found"}, status=400)

    ok, reason = await process_paid_order(order_id, wrapped_payload)
    code = 200 if ok else 400
    return web.json_response({"ok": ok, "reason": reason}, status=code)


async def healthcheck(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "boxvolt-bot"})


def collect_public_status_snapshot() -> dict[str, Any]:
    now_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    users_row = conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
    active_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM users
        WHERE subscription_end IS NOT NULL
          AND subscription_end != ''
          AND subscription_end > ?
        """,
        (now_at,),
    ).fetchone()
    pending_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM payments WHERE status = 'pending'"
    ).fetchone()
    paid_today_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(amount_rub), 0) AS revenue_rub
        FROM payments
        WHERE status = 'paid'
          AND paid_at >= ?
        """,
        ((dt.datetime.now() - dt.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S"),),
    ).fetchone()
    conn.close()

    provider = get_active_payment_provider()
    provider_ready = payment_provider_is_ready(provider)
    backup_last_success = get_app_meta(auto_backup_last_success_key())
    backup_last_attempt = get_app_meta(auto_backup_last_attempt_key())
    return {
        "ok": True,
        "service": "boxvolt-bot",
        "timestamp": now_at,
        "app_version": resolve_app_version(),
        "maintenance_mode": maintenance_mode_enabled(),
        "update_notify_mode": get_update_notify_mode(),
        "payment_provider": provider,
        "payment_provider_label": payment_provider_label(provider),
        "payment_provider_ready": provider_ready,
        "users_total": int(users_row["cnt"] or 0) if users_row else 0,
        "active_subscriptions": int(active_row["cnt"] or 0) if active_row else 0,
        "pending_orders": int(pending_row["cnt"] or 0) if pending_row else 0,
        "paid_24h_count": int(paid_today_row["cnt"] or 0) if paid_today_row else 0,
        "paid_24h_revenue_rub": float(paid_today_row["revenue_rub"] or 0.0) if paid_today_row else 0.0,
        "backup_last_success": backup_last_success,
        "backup_last_attempt": backup_last_attempt,
    }


def render_public_status_html(snapshot: dict[str, Any]) -> str:
    maintenance = bool(snapshot.get("maintenance_mode"))
    provider_ready = bool(snapshot.get("payment_provider_ready"))
    maintenance_badge = "🔴 Технические работы" if maintenance else "🟢 Рабочий режим"
    provider_badge = "🟢 Оплата доступна" if provider_ready else "🔴 Оплата недоступна"
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BoxVolt Status</title>
  <style>
    :root {{
      --bg:#06131a;
      --panel:#0e2430;
      --line:#1b3c4d;
      --text:#dff7ff;
      --muted:#8db8c8;
      --ok:#8ef7c4;
      --bad:#ffb8b8;
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0;
      font-family: "Segoe UI", "Trebuchet MS", sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, #0f2e3d, transparent 60%), var(--bg);
      color:var(--text);
      min-height:100vh;
      display:flex;
      align-items:flex-start;
      justify-content:center;
      padding:28px 12px;
    }}
    .card {{
      width:min(760px, 100%);
      background:linear-gradient(180deg, rgba(14,36,48,.95), rgba(9,26,34,.95));
      border:1px solid var(--line);
      border-radius:16px;
      padding:18px;
    }}
    h1 {{ margin:0 0 8px; font-size:28px; }}
    .meta {{ color:var(--muted); font-size:13px; margin-bottom:14px; }}
    .grid {{
      display:grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap:10px;
      margin-top:12px;
    }}
    .item {{
      border:1px solid var(--line);
      border-radius:12px;
      padding:10px;
      background:rgba(255,255,255,.02);
    }}
    .k {{ color:var(--muted); font-size:12px; }}
    .v {{ font-size:16px; margin-top:4px; }}
    .ok {{ color:var(--ok); }}
    .bad {{ color:var(--bad); }}
    .json {{
      display:inline-block;
      margin-top:14px;
      color:var(--text);
      text-decoration:none;
      border:1px solid var(--line);
      border-radius:999px;
      padding:8px 12px;
    }}
  </style>
</head>
<body>
  <main class="card">
    <h1>BoxVolt Status</h1>
    <div class="meta">Обновлено: {html.escape(str(snapshot.get("timestamp") or "-"))}</div>
    <div class="v {'bad' if maintenance else 'ok'}">{html.escape(maintenance_badge)}</div>
    <div class="v {'ok' if provider_ready else 'bad'}">{html.escape(provider_badge)}</div>
    <div class="grid">
      <div class="item"><div class="k">Провайдер оплаты</div><div class="v">{html.escape(str(snapshot.get("payment_provider_label") or "-"))}</div></div>
      <div class="item"><div class="k">Пользователи</div><div class="v">{int(snapshot.get("users_total") or 0)}</div></div>
      <div class="item"><div class="k">Активные подписки</div><div class="v">{int(snapshot.get("active_subscriptions") or 0)}</div></div>
      <div class="item"><div class="k">Ожидают оплаты</div><div class="v">{int(snapshot.get("pending_orders") or 0)}</div></div>
      <div class="item"><div class="k">Оплат за 24ч</div><div class="v">{int(snapshot.get("paid_24h_count") or 0)}</div></div>
      <div class="item"><div class="k">Выручка за 24ч</div><div class="v">{format_rub_value(float(snapshot.get("paid_24h_revenue_rub") or 0.0))} ₽</div></div>
      <div class="item"><div class="k">Последний backup</div><div class="v">{html.escape(str(snapshot.get("backup_last_success") or "-"))}</div></div>
      <div class="item"><div class="k">Режим update-уведомлений</div><div class="v">{html.escape(str(snapshot.get("update_notify_mode") or "-"))}</div></div>
    </div>
    <a class="json" href="/status.json" target="_blank" rel="noopener">Открыть JSON</a>
  </main>
</body>
</html>
"""


async def public_status_json(_: web.Request) -> web.Response:
    return web.json_response(collect_public_status_snapshot())


async def public_status_page(_: web.Request) -> web.Response:
    html_page = render_public_status_html(collect_public_status_snapshot())
    return web.Response(
        text=html_page,
        content_type="text/html",
        headers={"Cache-Control": "no-store"},
    )


def render_subscription_profile_html(
    telegram_id: int,
    username: str | None,
    subscription_end: str | None,
    subscription_url: str,
) -> str:
    active = has_active_subscription(subscription_end)
    expiry = parse_date(subscription_end)
    expiry_text = expiry.strftime("%d.%m.%Y %H:%M") if expiry else "Не активна"
    remaining_text = format_subscription_remaining(subscription_end)
    status_text = "🟢 Активна" if active else "🔴 Неактивна"
    status_hint = (
        "Подписка работает стабильно и обновляется автоматически."
        if active
        else "Подписка неактивна. Продлите доступ в боте."
    )
    support_url = f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}" if SUPPORT_CONTACT else ""
    username_text = f"@{username}" if username else str(telegram_id)
    country_text = server_country_label()
    sub_url_safe = html.escape(subscription_url)
    sub_url_encoded = quote(subscription_url, safe="")

    deep_links = {
        "happ": (
            [
                f"happ://add/{subscription_url}",
                f"happ://add/{sub_url_encoded}",
                f"happ://install-config/?url={sub_url_encoded}",
                f"happ://install-config?url={sub_url_encoded}",
            ]
            if sub_url_encoded
            else []
        ),
        "v2raytun": (
            [
                f"v2raytun://install-config?url={sub_url_encoded}",
                f"v2raytun://install-config/?url={sub_url_encoded}",
                f"v2raytun://import/{sub_url_encoded}",
                f"v2raytun://install-sub/?url={sub_url_encoded}",
            ]
            if sub_url_encoded
            else []
        ),
        "hiddify": (
            [
                f"hiddify://install-config?url={sub_url_encoded}",
                f"hiddify://install-config/?url={sub_url_encoded}",
            ]
            if sub_url_encoded
            else []
        ),
    }
    deep_links_json = json.dumps(deep_links, ensure_ascii=False)

    download_links = {
        "windows": "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe",
        "hiddify_windows": "https://github.com/hiddify/hiddify-app/releases/latest",
        "android_play": "https://play.google.com/store/apps/details?id=com.happproxy",
        "android_apk": "https://github.com/Happ-proxy/happ-android/releases/latest/download/Happ.apk",
        "ios": "https://apps.apple.com/us/app/happ-proxy-utility/id6504287215",
        "macos": "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ.macOS.universal.dmg",
        "v2raytun_ios": "https://apps.apple.com/app/v2raytun/id6476628951",
        "v2raytun_android": "https://play.google.com/store/apps/details?id=com.v2raytun.android",
    }

    qr_data_url = build_subscription_qr_data_url(subscription_url)
    qr_block_html = (
        f'<img id="subQr" class="qr-canvas" src="{html.escape(qr_data_url)}" '
        'alt="QR Subscription URL">'
        if qr_data_url
        else '<div class="qr-fallback">QR временно недоступен. Используйте URL ниже.</div>'
    )

    support_link_html = (
        f'<a class="btn btn-ghost" href="{html.escape(support_url)}" target="_blank" rel="noopener">'
        "Поддержка</a>"
        if support_url
        else ""
    )
    active_class = "ok" if active else "bad"

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BoxVolt Elite Access</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Exo+2:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg-top: #e3f2f2;
      --bg-bottom: #cde1df;
      --ink: #032d2c;
      --muted: #3f6461;
      --panel: rgba(2, 74, 74, 0.92);
      --panel-soft: rgba(3, 94, 95, 0.84);
      --card: rgba(2, 61, 63, 0.75);
      --line: rgba(179, 245, 235, 0.25);
      --ok: #8fffe0;
      --bad: #ffd9d9;
      --btn: rgba(216, 255, 247, 0.2);
      --btn-border: rgba(199, 255, 246, 0.55);
      --title: #ecfff8;
      --white: #f6fffc;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Exo 2", "Trebuchet MS", sans-serif;
      color: var(--white);
      background:
        radial-gradient(1400px 700px at 12% -10%, rgba(92, 197, 185, 0.34), transparent 64%),
        radial-gradient(900px 560px at 100% 0%, rgba(21, 96, 103, 0.42), transparent 62%),
        linear-gradient(180deg, var(--bg-top), var(--bg-bottom));
      min-height: 100vh;
      padding: 20px 14px 36px;
    }}
    .wrap {{
      max-width: 980px;
      margin: 0 auto;
      display: grid;
      gap: 14px;
    }}
    .hero {{
      position: relative;
      overflow: hidden;
      border-radius: 26px;
      background: linear-gradient(160deg, rgba(2, 72, 71, 0.95), rgba(3, 99, 102, 0.84));
      border: 1px solid var(--line);
      box-shadow: 0 22px 50px rgba(2, 42, 45, 0.25);
      padding: 18px;
    }}
    .hero::before {{
      content: "";
      position: absolute;
      width: 280px;
      height: 280px;
      right: -110px;
      top: -110px;
      border-radius: 40px;
      transform: rotate(18deg);
      background: rgba(170, 255, 240, 0.09);
      border: 1px solid rgba(183, 255, 246, 0.32);
      pointer-events: none;
    }}
    .top-nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }}
    .pill {{
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.02em;
      padding: 7px 14px;
      border-radius: 999px;
      background: var(--btn);
      border: 1px solid var(--btn-border);
      color: var(--title);
      text-decoration: none;
      backdrop-filter: blur(8px);
    }}
    .brand {{
      font-size: clamp(22px, 3.8vw, 34px);
      margin: 0 0 2px;
      color: var(--title);
      letter-spacing: 0.01em;
    }}
    .sub {{
      margin: 0;
      color: rgba(237, 255, 251, 0.88);
      font-size: 15px;
    }}
    .meta {{
      margin-top: 14px;
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    }}
    .state {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-weight: 700;
      font-size: 14px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--btn-border);
      background: rgba(214, 255, 246, 0.08);
    }}
    .state.ok {{ color: var(--ok); }}
    .state.bad {{ color: var(--bad); }}
    .stat {{
      border-radius: 16px;
      border: 1px solid var(--line);
      background: var(--card);
      padding: 12px;
      backdrop-filter: blur(9px);
    }}
    .k {{
      color: rgba(222, 255, 249, 0.73);
      font-size: 12px;
      margin-bottom: 5px;
      letter-spacing: 0.03em;
    }}
    .v {{
      font-size: 16px;
      font-weight: 600;
      color: var(--white);
    }}
    .panel {{
      border-radius: 24px;
      border: 1px solid var(--line);
      background: linear-gradient(170deg, var(--panel), var(--panel-soft));
      box-shadow: 0 18px 42px rgba(3, 44, 44, 0.22);
      padding: 18px;
    }}
    .panel h2 {{
      margin: 0 0 6px;
      font-size: 22px;
      color: var(--title);
    }}
    .panel p {{
      margin: 0;
      color: rgba(223, 255, 251, 0.82);
      line-height: 1.45;
    }}
    .hint {{
      margin: 14px 0 0;
      border-radius: 14px;
      border: 1px solid rgba(174, 246, 237, 0.33);
      background: rgba(205, 255, 243, 0.09);
      padding: 10px 12px;
      color: #e9fffb;
      font-weight: 500;
    }}
    .suburl {{
      width: 100%;
      margin-top: 12px;
      background: rgba(2, 43, 45, 0.7);
      color: #f1fffc;
      border: 1px solid rgba(194, 255, 246, 0.38);
      border-radius: 12px;
      padding: 11px 12px;
      font-size: 13px;
      outline: none;
    }}
    .qr-wrap {{
      margin-top: 14px;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 10px;
      padding: 12px;
      border: 1px solid rgba(187, 255, 246, 0.26);
      border-radius: 18px;
      background: rgba(4, 58, 61, 0.48);
    }}
    .qr-head {{
      color: rgba(224, 255, 250, 0.84);
      font-size: 13px;
      font-weight: 600;
      text-align: center;
    }}
    .qr-canvas {{
      width: 220px;
      max-width: 100%;
      height: auto;
      display: block;
      padding: 8px;
      border-radius: 12px;
      border: 1px solid rgba(178, 247, 237, 0.4);
      background: #ffffff;
    }}
    .qr-fallback {{
      width: 220px;
      max-width: 100%;
      min-height: 120px;
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      padding: 10px;
      border-radius: 12px;
      border: 1px dashed rgba(193, 255, 248, 0.5);
      color: rgba(223, 255, 248, 0.85);
      font-size: 13px;
      background: rgba(4, 43, 45, 0.55);
    }}
    .qr-note {{
      color: rgba(226, 255, 250, 0.73);
      font-size: 12px;
      text-align: center;
    }}
    .btn-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 10px;
    }}
    .btn {{
      appearance: none;
      border: 1px solid var(--btn-border);
      border-radius: 12px;
      padding: 10px 14px;
      font-weight: 700;
      letter-spacing: 0.01em;
      cursor: pointer;
      text-decoration: none;
      color: var(--title);
      background: var(--btn);
      backdrop-filter: blur(10px);
      transition: transform .18s ease, background .18s ease, border-color .18s ease;
    }}
    .btn:hover {{
      transform: translateY(-1px);
      background: rgba(217, 255, 247, 0.28);
      border-color: rgba(214, 255, 248, 0.84);
    }}
    .btn-ghost {{
      background: rgba(6, 63, 65, 0.35);
    }}
    .apps-grid {{
      margin-top: 14px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
    }}
    .app-card {{
      border: 1px solid rgba(189, 255, 247, 0.27);
      border-radius: 14px;
      background: rgba(3, 56, 58, 0.5);
      padding: 11px;
    }}
    .app-card h4 {{
      margin: 0 0 4px;
      font-size: 15px;
      color: #f1fffc;
    }}
    .app-card p {{
      margin: 0 0 10px;
      font-size: 12px;
      color: rgba(218, 255, 249, 0.76);
    }}
    .downloads-grid {{
      margin-top: 14px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
    }}
    .download-card {{
      border: 1px solid rgba(187, 255, 245, 0.3);
      border-radius: 16px;
      background: rgba(5, 60, 61, 0.55);
      padding: 12px;
    }}
    .download-card h4 {{
      margin: 0 0 4px;
      font-size: 16px;
      color: #effffb;
    }}
    .download-card p {{
      margin: 0 0 10px;
      font-size: 13px;
      color: rgba(218, 255, 249, 0.75);
    }}
    .download-links {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .steps {{
      margin-top: 14px;
      border-top: 1px dashed rgba(191, 255, 248, 0.33);
      padding-top: 12px;
    }}
    .steps h3 {{
      margin: 0 0 8px;
      font-size: 16px;
      color: #edfffb;
    }}
    .steps ol {{
      margin: 0;
      padding-left: 18px;
      color: rgba(220, 255, 250, 0.82);
      line-height: 1.4;
    }}
    .steps li {{
      margin: 5px 0;
    }}
    .platform-note {{
      margin-top: 10px;
      color: rgba(227, 255, 251, 0.86);
      font-size: 12px;
    }}
    .platform-hidden {{
      display: none !important;
    }}
    .toast {{
      position: fixed;
      left: 50%;
      bottom: 16px;
      transform: translateX(-50%) translateY(18px);
      padding: 10px 14px;
      border-radius: 12px;
      background: rgba(2, 45, 47, 0.9);
      border: 1px solid rgba(197, 255, 248, 0.4);
      color: #effffb;
      font-size: 13px;
      opacity: 0;
      pointer-events: none;
      transition: opacity .22s ease, transform .22s ease;
      z-index: 9;
    }}
    .toast.show {{
      opacity: 1;
      transform: translateX(-50%) translateY(0);
    }}
    @media (max-width: 680px) {{
      body {{ padding: 12px 10px 28px; }}
      .hero,
      .panel {{
        border-radius: 18px;
        padding: 14px;
      }}
      .brand {{ font-size: 24px; }}
      .suburl {{ font-size: 12px; }}
      .btn {{
        width: 100%;
        text-align: center;
      }}
      .btn-row {{
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <nav class="top-nav">
        <a href="#profile" class="pill">Профиль</a>
        <a href="#import" class="pill">Импорт</a>
        <a href="#downloads" class="pill">Приложения</a>
      </nav>
      <h1 class="brand">BoxVolt Elite Access</h1>
      <p class="sub">Премиум-подписка VLESS Reality • быстрый импорт • автообновление</p>
      <div class="meta" id="profile">
        <div class="stat">
          <div class="k">Статус</div>
          <div class="v"><span class="state {active_class}">{status_text}</span></div>
        </div>
        <div class="stat">
          <div class="k">Пользователь</div>
          <div class="v">{html.escape(username_text)}</div>
        </div>
        <div class="stat">
          <div class="k">Telegram ID</div>
          <div class="v">{telegram_id}</div>
        </div>
        <div class="stat">
          <div class="k">Сервер</div>
          <div class="v">{html.escape(country_text)}</div>
        </div>
        <div class="stat">
          <div class="k">Истекает</div>
          <div class="v">{html.escape(expiry_text)}</div>
        </div>
        <div class="stat">
          <div class="k">Осталось</div>
          <div class="v">{html.escape(remaining_text)}</div>
        </div>
      </div>
      <div class="hint">{html.escape(status_hint)} Автообновление: каждые {SUBSCRIPTION_UPDATE_INTERVAL_HOURS} ч.</div>
    </section>

    <section id="import" class="panel">
      <h2>Быстрый импорт подписки</h2>
      <p>Если кнопка добавления не открыла приложение, ссылка ниже автоматически скопируется в буфер.</p>

      <div class="btn-row">
        <button class="btn" type="button" data-platforms="ios,android,windows,macos" onclick="openSubscriptionApp('happ')">Happ: добавить подписку</button>
        <button class="btn btn-ghost" type="button" data-platforms="ios,android,windows,macos,linux" onclick="openSubscriptionApp('v2raytun')">V2rayTun: добавить подписку</button>
        <button class="btn btn-ghost" type="button" data-platforms="ios,android,windows,macos,linux" onclick="openSubscriptionApp('hiddify')">Hiddify: добавить подписку</button>
      </div>
      <div id="platformHint" class="platform-note"></div>

      <input id="subUrl" class="suburl" value="{sub_url_safe}" readonly>
      <div class="btn-row">
        <button class="btn" type="button" onclick="copySubUrl()">Скопировать ссылку</button>
        {support_link_html}
      </div>

      <div class="qr-wrap">
        <div class="qr-head">QR для Subscription URL</div>
        {qr_block_html}
        <div class="qr-note">Сканируйте QR в приложении через Import / Scan.</div>
      </div>

      <div class="apps-grid">
        <div class="app-card" data-platforms="ios,android,windows,macos">
          <h4>Happ</h4>
          <p>Рекомендуется для iOS/Android/macOS/Windows.</p>
          <button class="btn btn-ghost" type="button" onclick="openSubscriptionApp('happ')">Открыть Happ</button>
        </div>
        <div class="app-card" data-platforms="ios,android,windows,macos,linux">
          <h4>Hiddify + V2rayTun</h4>
          <p>Альтернативные клиенты для быстрого импорта подписки.</p>
          <div class="btn-row">
            <button class="btn btn-ghost" type="button" onclick="openSubscriptionApp('hiddify')">Открыть Hiddify</button>
            <button class="btn btn-ghost" type="button" onclick="openSubscriptionApp('v2raytun')">Открыть V2rayTun</button>
          </div>
        </div>
      </div>

      <div class="steps">
        <h3>Подключение за 30 секунд</h3>
        <ol>
          <li>Установите приложение на устройство.</li>
          <li>Нажмите кнопку добавления подписки или скопируйте URL вручную.</li>
          <li>Выберите профиль BoxVolt и нажмите Connect.</li>
        </ol>
      </div>
    </section>

    <section id="downloads" class="panel">
      <h2>Скачать приложения</h2>
      <p>Выберите платформу. Кнопки ведут на официальные страницы/релизы.</p>
      <div class="downloads-grid">
        <article class="download-card" data-platforms="windows">
          <h4>Windows</h4>
          <p>Happ Desktop (x64) и Hiddify для Windows.</p>
          <div class="download-links">
            <a class="btn btn-ghost" href="{html.escape(download_links['windows'])}" target="_blank" rel="noopener">Скачать Happ</a>
            <a class="btn btn-ghost" href="{html.escape(download_links['hiddify_windows'])}" target="_blank" rel="noopener">Скачать Hiddify</a>
          </div>
        </article>
        <article class="download-card" data-platforms="android">
          <h4>Android</h4>
          <p>Google Play и APK-установка.</p>
          <div class="download-links">
            <a class="btn btn-ghost" href="{html.escape(download_links['android_play'])}" target="_blank" rel="noopener">Happ (Play)</a>
            <a class="btn btn-ghost" href="{html.escape(download_links['android_apk'])}" target="_blank" rel="noopener">Happ (APK)</a>
            <a class="btn btn-ghost" href="{html.escape(download_links['v2raytun_android'])}" target="_blank" rel="noopener">V2rayTun (Play)</a>
          </div>
        </article>
        <article class="download-card" data-platforms="ios">
          <h4>iPhone (iOS)</h4>
          <p>Установка через App Store.</p>
          <div class="download-links">
            <a class="btn btn-ghost" href="{html.escape(download_links['ios'])}" target="_blank" rel="noopener">Happ (App Store)</a>
            <a class="btn btn-ghost" href="{html.escape(download_links['v2raytun_ios'])}" target="_blank" rel="noopener">V2rayTun (App Store)</a>
          </div>
        </article>
        <article class="download-card" data-platforms="macos">
          <h4>macOS</h4>
          <p>Happ Desktop universal DMG.</p>
          <div class="download-links">
            <a class="btn btn-ghost" href="{html.escape(download_links['macos'])}" target="_blank" rel="noopener">Скачать Happ для macOS</a>
          </div>
        </article>
      </div>
    </section>
  </main>
  <div id="toast" class="toast" aria-live="polite"></div>
  <script>
    const DEEP_LINKS = {deep_links_json};

    function detectPlatformFamily() {{
      const ua = (navigator.userAgent || "").toLowerCase();
      const platform = (navigator.platform || "").toLowerCase();
      if (/iphone|ipad|ipod/.test(ua) || /iphone|ipad|ipod/.test(platform)) return "ios";
      if (/android/.test(ua)) return "android";
      if (/win/.test(platform) || /windows/.test(ua)) return "windows";
      if (/mac/.test(platform) || /mac os/.test(ua)) return "macos";
      if (/linux/.test(platform) || /linux/.test(ua)) return "linux";
      return "unknown";
    }}

    function prettyPlatformLabel(platform) {{
      if (platform === "ios") return "iPhone / iOS";
      if (platform === "android") return "Android";
      if (platform === "windows") return "Windows";
      if (platform === "macos") return "macOS";
      if (platform === "linux") return "Linux";
      return "не определена";
    }}

    function applyPlatformVisibility() {{
      const platform = detectPlatformFamily();
      const nodes = document.querySelectorAll("[data-platforms]");
      if (platform === "unknown") {{
        const hint = document.getElementById("platformHint");
        if (hint) {{
          hint.textContent = "Платформа не определена. Показаны все варианты.";
        }}
        return;
      }}
      nodes.forEach((node) => {{
        const raw = String(node.getAttribute("data-platforms") || "");
        const allowed = raw
          .split(",")
          .map((x) => x.trim().toLowerCase())
          .filter(Boolean);
        const visible = allowed.length === 0 || allowed.includes(platform) || allowed.includes("all");
        node.classList.toggle("platform-hidden", !visible);
      }});
      const hint = document.getElementById("platformHint");
      if (hint) {{
        hint.textContent = "Определена платформа: " + prettyPlatformLabel(platform);
      }}
    }}

    function showToast(text) {{
      const toast = document.getElementById("toast");
      if (!toast) return;
      toast.textContent = text;
      toast.classList.add("show");
      clearTimeout(window.__bvToastTimer);
      window.__bvToastTimer = setTimeout(() => {{
        toast.classList.remove("show");
      }}, 2400);
    }}

    async function copySubUrl(needToast = true) {{
      const input = document.getElementById("subUrl");
      const text = input.value || "";
      try {{
        if (navigator.clipboard && navigator.clipboard.writeText) {{
          await navigator.clipboard.writeText(text);
        }} else {{
          input.select();
          document.execCommand("copy");
        }}
        if (needToast) {{
          showToast("Ссылка подписки скопирована");
        }}
        return true;
      }} catch (e) {{
        if (needToast) {{
          showToast("Не удалось скопировать. Выделите ссылку вручную.");
        }}
        return false;
      }}
    }}

    function triggerDeepLink(url) {{
      const a = document.createElement("a");
      a.href = url;
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      a.remove();

      const probe = document.createElement("iframe");
      probe.style.display = "none";
      probe.src = url;
      document.body.appendChild(probe);
      setTimeout(() => probe.remove(), 650);
    }}

    async function openSubscriptionApp(appKey) {{
      const links = Array.isArray(DEEP_LINKS[appKey]) ? DEEP_LINKS[appKey] : [];
      if (!links.length) {{
        await copySubUrl(false);
        showToast("Ссылка скопирована. Добавьте её в приложение вручную.");
        return;
      }}

      showToast("Пытаемся открыть приложение…");
      const maxAttempts = Math.min(links.length, 4);
      let attempt = 0;
      const tryNext = async () => {{
        if (attempt >= maxAttempts) {{
          if (!document.hidden) {{
            await copySubUrl(false);
            showToast("Приложение не открылось. Ссылка уже скопирована.");
          }}
          return;
        }}
        const link = String(links[attempt] || "").trim();
        attempt += 1;
        if (!link) {{
          await tryNext();
          return;
        }}
        triggerDeepLink(link);
        setTimeout(async () => {{
          if (document.hidden) return;
          await tryNext();
        }}, 700);
      }};
      await tryNext();
    }}

    applyPlatformVisibility();
  </script>
</body>
</html>
"""

def resolve_subscription_request(request: web.Request) -> tuple[int | None, str]:
    short_token = str(request.match_info.get("sub_token") or "").strip()
    if short_token:
        telegram_id = extract_telegram_id_from_subscription_token(short_token)
        return telegram_id, short_token

    telegram_id_raw = str(request.match_info.get("telegram_id") or "").strip()
    token = str(request.match_info.get("token") or "").strip()
    if telegram_id_raw.isdigit():
        return int(telegram_id_raw), token
    return None, token


async def subscription_feed(request: web.Request) -> web.Response:
    telegram_id, token = resolve_subscription_request(request)
    if not telegram_id or not token:
        return web.Response(text="forbidden", status=403)

    user = get_user(telegram_id)
    if not user:
        return web.Response(text="user_not_found", status=404)
    if not is_valid_subscription_token(telegram_id, token, user_uuid=user["vless_uuid"]):
        return web.Response(text="forbidden", status=403)
    if not has_active_subscription(user["subscription_end"]):
        return web.Response(text="subscription_expired", status=403)

    try:
        user_uuid = await ensure_vless_uuid(
            telegram_id,
            user["vless_uuid"],
            user["subscription_end"],
        )
        main_link = await generate_vless_link_with_options(
            user_uuid=user_uuid,
            display_name=server_country_label(),
        )
        link_map: dict[str, str] = {"main": main_link}

        speed_uuid = user_uuid
        speed_profile: dict[str, str] | None = None
        speed_port = SERVER_PORT
        speed_flow = SPEED_PROFILE_FLOW if SPEED_PROFILE_FLOW != "" else XUI_FLOW
        speed_inbound_synced = False
        speed_link = ""

        if SPEED_PROFILE_ENABLED:
            if SPEED_INBOUND_ID > 0 and SPEED_INBOUND_ID != INBOUND_ID:
                try:
                    speed_uuid, speed_inbound_obj = await xui_upsert_client_for_inbound(
                        inbound_id=SPEED_INBOUND_ID,
                        telegram_id=telegram_id,
                        preferred_uuid=user_uuid,
                        subscription_end=user["subscription_end"],
                        flow_override=speed_flow if SPEED_PROFILE_FLOW != "" else None,
                        cache_main_reality=False,
                        email_override=speed_inbound_email(telegram_id),
                    )
                    speed_profile = extract_reality_profile_from_inbound(speed_inbound_obj)
                    speed_port = non_negative_int(speed_inbound_obj.get("port")) or SERVER_PORT
                    speed_inbound_synced = True
                except Exception as exc:  # noqa: BLE001
                    print(f"[xui] Failed to sync speed inbound for {telegram_id}: {exc}")

            speed_link = await generate_vless_link_with_options(
                user_uuid=speed_uuid,
                display_name=speed_profile_display_name(),
                flow_override=speed_flow if speed_flow else None,
                reality_profile=speed_profile,
                server_port=speed_port,
            )
            if speed_link:
                link_map["speed"] = speed_link

        reserve_link = ""
        reserve_inbound_id = ROUTE_RESERVE_INBOUND_ID
        if reserve_inbound_id > 0 and reserve_inbound_id != INBOUND_ID:
            if speed_inbound_synced and reserve_inbound_id == SPEED_INBOUND_ID and speed_link:
                reserve_link = speed_link
            else:
                try:
                    reserve_uuid, reserve_inbound_obj = await xui_upsert_client_for_inbound(
                        inbound_id=reserve_inbound_id,
                        telegram_id=telegram_id,
                        preferred_uuid=user_uuid,
                        subscription_end=user["subscription_end"],
                        flow_override=XUI_FLOW if XUI_FLOW != "" else None,
                        cache_main_reality=False,
                        email_override=reserve_inbound_email(telegram_id),
                    )
                    reserve_profile = extract_reality_profile_from_inbound(reserve_inbound_obj)
                    reserve_port = non_negative_int(reserve_inbound_obj.get("port")) or SERVER_PORT
                    reserve_link = await generate_vless_link_with_options(
                        user_uuid=reserve_uuid,
                        display_name=reserve_profile_display_name(),
                        flow_override=XUI_FLOW if XUI_FLOW else None,
                        reality_profile=reserve_profile,
                        server_port=reserve_port,
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"[xui] Failed to sync reserve inbound for {telegram_id}: {exc}")
        elif speed_link:
            reserve_link = speed_link
        if reserve_link:
            link_map["reserve"] = reserve_link

        if YOUTUBE_PROFILE_ENABLED:
            yt_flow = YOUTUBE_PROFILE_FLOW if YOUTUBE_PROFILE_FLOW != "" else XUI_FLOW
            yt_uuid = user_uuid
            yt_profile: dict[str, str] | None = None
            yt_port = SERVER_PORT

            if (
                speed_inbound_synced
                and YOUTUBE_INBOUND_ID > 0
                and YOUTUBE_INBOUND_ID == SPEED_INBOUND_ID
            ):
                yt_uuid = speed_uuid
                yt_profile = speed_profile
                yt_port = speed_port
            elif YOUTUBE_INBOUND_ID > 0 and YOUTUBE_INBOUND_ID != INBOUND_ID:
                try:
                    yt_uuid, yt_inbound_obj = await xui_upsert_client_for_inbound(
                        inbound_id=YOUTUBE_INBOUND_ID,
                        telegram_id=telegram_id,
                        preferred_uuid=user_uuid,
                        subscription_end=user["subscription_end"],
                        flow_override=yt_flow if YOUTUBE_PROFILE_FLOW != "" else None,
                        cache_main_reality=False,
                        email_override=youtube_inbound_email(telegram_id),
                    )
                    yt_profile = extract_reality_profile_from_inbound(yt_inbound_obj)
                    yt_port = non_negative_int(yt_inbound_obj.get("port")) or SERVER_PORT
                except Exception as exc:  # noqa: BLE001
                    print(f"[xui] Failed to sync youtube inbound for {telegram_id}: {exc}")

            yt_link = await generate_vless_link_with_options(
                user_uuid=yt_uuid,
                display_name=youtube_profile_display_name(),
                flow_override=yt_flow if yt_flow else None,
                reality_profile=yt_profile,
                server_port=yt_port,
            )
            if yt_link:
                link_map["youtube"] = yt_link

        preferred_mode = effective_user_route_mode(user, telegram_id)
        preferred_order = ["main", "speed", "reserve", "youtube"]
        if preferred_mode == "reserve" and link_map.get("reserve"):
            preferred_order = ["reserve", "main", "speed", "youtube"]

        seen_links: set[str] = set()
        vless_links: list[str] = []
        for key in preferred_order:
            link = str(link_map.get(key) or "").strip()
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            vless_links.append(link)
        for link in link_map.values():
            normalized = str(link or "").strip()
            if not normalized or normalized in seen_links:
                continue
            seen_links.add(normalized)
            vless_links.append(normalized)
    except Exception as exc:  # noqa: BLE001
        return web.Response(text=f"key_generation_failed: {exc}", status=500)

    upload_bytes = 0
    download_bytes = 0
    total_bytes = subscription_total_bytes()
    try:
        upload_bytes, download_bytes, total_bytes = await xui_get_client_traffic_bytes(
            telegram_id=telegram_id,
            client_uuid=user_uuid,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[xui] Failed to fetch traffic stats for {telegram_id}: {exc}")

    headers = {
        "Cache-Control": "no-store",
        "profile-title": build_profile_title_header(),
        "profile-update-interval": str(SUBSCRIPTION_UPDATE_INTERVAL_HOURS),
        "subscription-userinfo": (
            f"upload={upload_bytes}; download={download_bytes}; total={total_bytes}; "
            f"expire={subscription_expire_unix(user['subscription_end'])}"
        ),
        "status": build_subscription_status_header(user["subscription_end"]),
    }

    profile_page_url = build_subscription_profile_url(telegram_id)
    if profile_page_url:
        headers["profile-web-page-url"] = profile_page_url
    if SUPPORT_CONTACT:
        headers["support-url"] = f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}"

    return web.Response(
        text="\n".join(vless_links) + "\n",
        headers=headers,
        content_type="text/plain",
    )


async def subscription_profile_page(request: web.Request) -> web.Response:
    telegram_id, token = resolve_subscription_request(request)
    if not telegram_id or not token:
        return web.Response(text="forbidden", status=403)

    user = get_user(telegram_id)
    if not user:
        return web.Response(text="user_not_found", status=404)
    if not is_valid_subscription_token(telegram_id, token, user_uuid=user["vless_uuid"]):
        return web.Response(text="forbidden", status=403)

    html_page = render_subscription_profile_html(
        telegram_id=telegram_id,
        username=user["username"],
        subscription_end=user["subscription_end"],
        subscription_url=build_subscription_url(telegram_id),
    )
    return web.Response(
        text=html_page,
        content_type="text/html",
        headers={"Cache-Control": "no-store"},
    )


def webapp_error(error: str, status: int = 400) -> web.Response:
    return web.json_response({"ok": False, "error": error}, status=status)


def webapp_auth_error_status(error: str) -> int:
    if error in {"bad_signature", "init_data_expired"}:
        return 401
    return 400


def validate_webapp_admin_init_data(init_data: str) -> tuple[bool, int | None, str, int]:
    ok, data, reason = validate_webapp_init_data(init_data)
    if not ok or not data:
        return False, None, reason, webapp_auth_error_status(reason)

    telegram_id = int(data["user_obj"]["id"])
    if not is_admin_user(telegram_id):
        return False, None, "forbidden", 403

    return True, telegram_id, "ok", 200


async def payment_success_page(request: web.Request) -> web.Response:
    order_id = str(request.query.get("order_id") or "").strip().upper()
    if not ORDER_ID_RE.fullmatch(order_id):
        order_id = ""

    start_payload = str(request.query.get("start") or "").strip()
    if not start_payload and order_id:
        start_payload = build_order_start_payload(order_id)

    provider_raw = str(request.query.get("provider") or "").strip()
    provider_label = payment_provider_label(provider_raw) if provider_raw else "платежа"

    if not bot_public_username_hint():
        await get_bot_public_username()

    mini_app_url = build_bot_startapp_url(start_payload) or build_bot_startapp_url("")
    bot_start_url = build_bot_start_url(start_payload) or build_bot_start_url("")
    webapp_url = (
        build_webapp_order_url(order_id, auto_pay=False) if order_id else build_webapp_tab_url("plans")
    )
    if not webapp_url:
        webapp_url = (WEBAPP_PUBLIC_URL or "").strip() or "https://connect.boxvolt.shop/webapp"

    redirect_target = bot_start_url or mini_app_url or webapp_url
    order_line = f"Заказ: <b>{html.escape(order_id)}</b>" if order_id else "Заказ найден."
    auto_line = "Сейчас откроем Telegram и проверим статус оплаты."
    if not redirect_target:
        auto_line = "Не удалось подобрать ссылку авто-возврата. Откройте бота вручную."

    page_html = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>BoxVolt • Оплата</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg:#051522;
      --card:#0b2335;
      --line:#1d4b67;
      --text:#e8f4ff;
      --muted:#9ec2dd;
      --accent:#3ec7ff;
      --btn:#0f2d43;
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
      background:
        radial-gradient(800px 420px at 15% -10%, rgba(62,199,255,.24), transparent 60%),
        radial-gradient(720px 420px at 90% 110%, rgba(110,246,186,.12), transparent 55%),
        var(--bg);
      color:var(--text); font-family: "Segoe UI", "Tahoma", sans-serif;
      padding:18px;
    }}
    .card {{
      width:min(560px, 96vw);
      border:1px solid var(--line);
      border-radius:16px;
      background:linear-gradient(180deg, rgba(14,39,59,.98), rgba(8,26,40,.98));
      padding:18px;
      box-shadow:0 22px 60px rgba(0,0,0,.45);
    }}
    h1 {{ margin:0 0 8px; font-size:22px; }}
    p {{ margin:8px 0; line-height:1.45; color:var(--muted); }}
    .row {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }}
    .btn {{
      display:inline-flex; align-items:center; justify-content:center;
      text-decoration:none; color:var(--text); border:1px solid var(--line);
      background:var(--btn); border-radius:12px; padding:10px 14px;
      font-weight:700; font-size:14px;
    }}
    .btn.primary {{
      background:linear-gradient(135deg, #1f8ec0, #37c5ff);
      color:#032338; border:0;
    }}
    .hint {{ font-size:13px; margin-top:10px; }}
  </style>
</head>
<body>
  <section class="card">
    <h1>✅ Оплата принята</h1>
    <p>Провайдер: <b>{html.escape(provider_label)}</b></p>
    <p>{order_line}</p>
    <p>{html.escape(auto_line)}</p>
    <div class="row">
      <a class="btn primary" href="{html.escape(mini_app_url or bot_start_url or webapp_url)}">🧩 Открыть в Telegram</a>
      <a class="btn" href="{html.escape(bot_start_url or webapp_url)}">🤖 Открыть бота</a>
      <a class="btn" href="{html.escape(webapp_url)}">🌐 Открыть WebApp</a>
    </div>
    <p class="hint">
      Если открылся браузер без Telegram, нажмите «Открыть в Telegram».
      Без Telegram Mini App не получает initData и режим оплаты будет ограничен.
    </p>
  </section>
  <script>
    (function() {{
      const target = {json.dumps(redirect_target)};
      if (!target) return;
      setTimeout(() => {{
        window.location.href = target;
      }}, 650);
    }})();
  </script>
</body>
</html>"""
    return web.Response(
        text=page_html,
        content_type="text/html",
        headers={"Cache-Control": "no-store"},
    )


async def webapp_page(_: web.Request) -> web.Response:
    if not WEBAPP_TEMPLATE_PATH.exists():
        return web.Response(text="WebApp template not found", status=500)

    html = WEBAPP_TEMPLATE_PATH.read_text(encoding="utf-8")
    return web.Response(
        text=html,
        content_type="text/html",
        headers={"Cache-Control": "no-store"},
    )


async def webapp_plans_api(_: web.Request) -> web.Response:
    plans = get_active_plans()
    return web.json_response(
        {
            "ok": True,
            "payment_enabled": payment_provider_is_ready(),
            "payment_provider": get_active_payment_provider(),
            "payment_provider_label": payment_provider_label(),
            "cryptobot_enabled": payment_provider_is_ready(CRYPTOBOT_PROVIDER),
            "cryptobot_label": payment_provider_label(CRYPTOBOT_PROVIDER),
            "lzt_enabled": payment_provider_is_ready(LZT_PROVIDER),
            "lzt_label": payment_provider_label(LZT_PROVIDER),
            "secondary_payment_enabled": bool(SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL),
            "secondary_payment_label": SECONDARY_PAYMENT_LABEL,
            "plans": [serialize_plan(plan) for plan in plans.values()],
        }
    )


async def webapp_me_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    user_obj = data["user_obj"]
    telegram_id = int(user_obj["id"])
    username = user_obj.get("username")

    upsert_user(telegram_id, username)
    user = get_user(telegram_id)
    pending = get_latest_pending_payment(telegram_id)
    active_promo = get_user_active_promocode(telegram_id)
    last_paid_plan = get_last_paid_plan_for_user(telegram_id)

    pending_order = None
    if pending:
        pending_order_id = str(pending["order_id"])
        pending_amount = float(pending["amount_rub"])
        pending_amount_int = int(round(pending_amount))
        promo_code = str(pending["promo_code"] or "").strip()
        promo_discount = int(pending["promo_discount_rub"] or 0)
        pending_provider = normalize_payment_provider(pending["provider"])
        links = await build_payment_links_for_order(
            pending_order_id,
            pending_amount_int,
            pending_provider,
            telegram_id=telegram_id,
            username=username,
        )
        pending_order = {
            "order_id": pending_order_id,
            "amount_rub": pending_amount,
            "base_amount_rub": float(pending["base_amount_rub"] or pending_amount),
            "days": int(pending["days"]),
            "plan_code": pending["plan_code"],
            "provider": pending_provider,
            "provider_label": payment_provider_label(pending_provider),
            "promo_code": promo_code if promo_code else None,
            "promo_discount_rub": promo_discount,
            "created_at": pending["created_at"],
            "expires_at": payment_expires_at_str(pending["created_at"]),
            "payment_url": links["payment_url"],
            "donatepay_payment_url": links["donatepay_payment_url"],
            "cryptobot_payment_url": links["cryptobot_payment_url"],
            "lzt_payment_url": links["lzt_payment_url"],
            "secondary_payment_url": links["secondary_payment_url"],
            "cryptobot_payment_label": payment_provider_label(CRYPTOBOT_PROVIDER),
            "lzt_payment_label": payment_provider_label(LZT_PROVIDER),
            "secondary_payment_label": SECONDARY_PAYMENT_LABEL,
        }

    return web.json_response(
        {
            "ok": True,
            "user": {
                "id": telegram_id,
                "username": username,
                "first_name": user_obj.get("first_name"),
                "last_name": user_obj.get("last_name"),
                "is_admin": is_admin_user(telegram_id),
            },
            "subscription": {
                "active": has_active_subscription(user["subscription_end"] if user else None),
                "subscription_end": user["subscription_end"] if user else None,
            },
            "active_promo": active_promo,
            "pending_order": pending_order,
            "last_paid_plan": serialize_plan(last_paid_plan) if last_paid_plan else None,
        }
    )


async def webapp_create_order_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    if not payment_provider_is_ready():
        return webapp_error("payment_not_configured", 503)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    plan_code = str(body.get("plan_code") or "")
    plans = get_active_plans()
    plan = plans.get(plan_code)
    if not plan:
        return webapp_error("unknown_plan", 400)

    user_obj = data["user_obj"]
    telegram_id = int(user_obj["id"])
    username = user_obj.get("username")
    upsert_user(telegram_id, username)
    if not is_admin_user(telegram_id) and is_user_blacklisted(telegram_id):
        return web.json_response(
            {"ok": False, "error": "user_blacklisted"},
            status=403,
        )

    retry_after, retry_reason = order_create_retry_state(telegram_id)
    if retry_after > 0:
        log_suspicious_flag(
            telegram_id,
            "order_rate_limited",
            f"context=webapp reason={retry_reason} retry_after={retry_after}",
        )
        return web.json_response(
            {
                "ok": False,
                "error": "order_rate_limited",
                "retry_after_seconds": retry_after,
            },
            status=429,
        )

    final_amount, promo = calculate_plan_price_for_user(telegram_id, plan)
    order_id = create_payment_order(
        telegram_id=telegram_id,
        plan=plan,
        amount_rub=final_amount,
        promo_code=promo["code"] if promo else None,
        promo_discount_rub=promo["discount_rub"] if promo else 0,
    )
    payment = get_payment(order_id, apply_expiry=False)
    provider = payment["provider"] if payment else get_active_payment_provider()
    links = await build_payment_links_for_order(
        order_id,
        final_amount,
        provider,
        telegram_id=telegram_id,
        username=username,
    )
    order_plan = serialize_plan(plan)
    order_plan["amount_rub"] = final_amount
    order_plan["base_amount_rub"] = plan.amount_rub
    order_plan["promo"] = promo

    return web.json_response(
        {
            "ok": True,
            "order_id": order_id,
            "payment_url": links["payment_url"],
            "donatepay_payment_url": links["donatepay_payment_url"],
            "cryptobot_payment_url": links["cryptobot_payment_url"],
            "lzt_payment_url": links["lzt_payment_url"],
            "secondary_payment_url": links["secondary_payment_url"],
            "cryptobot_payment_label": payment_provider_label(CRYPTOBOT_PROVIDER),
            "lzt_payment_label": payment_provider_label(LZT_PROVIDER),
            "secondary_payment_label": SECONDARY_PAYMENT_LABEL,
            "payment_provider": normalize_payment_provider(provider),
            "payment_provider_label": payment_provider_label(provider),
            "plan": order_plan,
            "expires_at": payment_expires_at_str(payment["created_at"] if payment else now_str()),
        }
    )


async def webapp_order_status_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    order_id = str(body.get("order_id") or "").strip().upper()
    if not order_id:
        return webapp_error("missing_order_id", 400)

    user_obj = data["user_obj"]
    telegram_id = int(user_obj["id"])
    payment = get_payment(order_id, apply_expiry=True)
    if not payment or int(payment["telegram_id"]) != telegram_id:
        return webapp_error("order_not_found", 404)

    if payment["status"] == "pending":
        try:
            await sync_pending_payment_order(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[webapp] sync order status error for {order_id}: {exc}")
        payment = get_payment(order_id, apply_expiry=True)
        if not payment or int(payment["telegram_id"]) != telegram_id:
            return webapp_error("order_not_found", 404)

    user = get_user(telegram_id)
    links = await build_payment_links_for_order(
        order_id,
        int(round(float(payment["amount_rub"]))),
        payment["provider"],
        telegram_id=telegram_id,
        username=user_obj.get("username"),
    )
    return web.json_response(
        {
            "ok": True,
            "order": {
                "order_id": payment["order_id"],
                "status": payment["status"],
                "amount_rub": float(payment["amount_rub"]),
                "base_amount_rub": float(payment["base_amount_rub"] or payment["amount_rub"]),
                "days": int(payment["days"]),
                "plan_code": payment["plan_code"],
                "promo_code": payment["promo_code"],
                "promo_discount_rub": int(payment["promo_discount_rub"] or 0),
                "provider": normalize_payment_provider(payment["provider"]),
                "provider_label": payment_provider_label(payment["provider"]),
                "payment_url": links["payment_url"],
                "donatepay_payment_url": links["donatepay_payment_url"],
                "cryptobot_payment_url": links["cryptobot_payment_url"],
                "lzt_payment_url": links["lzt_payment_url"],
                "secondary_payment_url": links["secondary_payment_url"],
                "cryptobot_payment_label": payment_provider_label(CRYPTOBOT_PROVIDER),
                "lzt_payment_label": payment_provider_label(LZT_PROVIDER),
                "secondary_payment_label": SECONDARY_PAYMENT_LABEL,
                "paid_at": payment["paid_at"],
                "created_at": payment["created_at"],
                "expires_at": payment_expires_at_str(payment["created_at"]),
            },
            "subscription_end": user["subscription_end"] if user else None,
        }
    )


async def webapp_activate_promocode_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    promo_code = normalize_promo_code(str(body.get("promo_code") or ""))
    if not promo_code:
        return webapp_error("missing_promo_code", 400)

    telegram_id = int(data["user_obj"]["id"])
    username = data["user_obj"].get("username")
    upsert_user(telegram_id, username)

    activated, activate_reason, active = activate_promocode_for_user(telegram_id, promo_code)
    if not activated:
        status = 400
        if activate_reason == "promo_not_found":
            status = 404
        if activate_reason in {"promo_limit_reached", "promo_already_activated"}:
            status = 409
        return webapp_error(activate_reason, status)

    return web.json_response({"ok": True, "promo": active})


async def webapp_cancel_order_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    order_id = str(body.get("order_id") or "").strip().upper()
    if not order_id:
        return webapp_error("missing_order_id", 400)

    telegram_id = int(data["user_obj"]["id"])
    cancelled, cancel_reason = cancel_payment_order(
        order_id,
        telegram_id=telegram_id,
        reason="cancelled_from_webapp",
    )
    if not cancelled:
        if cancel_reason == "order_not_found":
            return webapp_error("order_not_found", 404)
        if cancel_reason.startswith("not_pending:"):
            return webapp_error("order_not_pending", 409)
        return webapp_error("cancel_failed", 400)

    return web.json_response({"ok": True, "order_id": order_id, "status": "cancelled"})


async def webapp_repeat_order_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    telegram_id = int(data["user_obj"]["id"])
    username = data["user_obj"].get("username")
    upsert_user(telegram_id, username)
    if not is_admin_user(telegram_id) and is_user_blacklisted(telegram_id):
        return web.json_response(
            {"ok": False, "error": "user_blacklisted"},
            status=403,
        )

    retry_after, retry_reason = order_create_retry_state(telegram_id)
    if retry_after > 0:
        log_suspicious_flag(
            telegram_id,
            "order_rate_limited",
            f"context=webapp_repeat reason={retry_reason} retry_after={retry_after}",
        )
        return web.json_response(
            {
                "ok": False,
                "error": "order_rate_limited",
                "retry_after_seconds": retry_after,
            },
            status=429,
        )

    source_order_id = str(body.get("source_order_id") or "").strip().upper()
    if not source_order_id:
        latest = get_latest_recreatable_payment(telegram_id)
        if not latest:
            return webapp_error("source_order_not_found", 404)
        source_order_id = str(latest["order_id"] or "").strip().upper()
    if not source_order_id:
        return webapp_error("source_order_not_found", 404)

    recreated, recreate_reason, payload = recreate_payment_order_from_previous(
        source_order_id,
        telegram_id,
        username=username,
    )
    if not recreated or not payload:
        if recreate_reason == "order_not_found":
            return webapp_error("source_order_not_found", 404)
        if recreate_reason == "forbidden":
            return webapp_error("forbidden", 403)
        if recreate_reason in {"order_not_recreatable", "plan_not_found"}:
            return webapp_error(recreate_reason, 409)
        return webapp_error("repeat_order_failed", 400)

    plan = payload["plan"]
    plan_data = serialize_plan(plan)
    plan_data["amount_rub"] = int(payload["amount_rub"])
    plan_data["base_amount_rub"] = plan.amount_rub
    plan_data["promo"] = payload.get("promo")
    links = await build_payment_links_for_order(
        str(payload["order_id"]),
        int(payload["amount_rub"]),
        str(payload["provider"]),
        telegram_id=telegram_id,
        username=username,
    )

    return web.json_response(
        {
            "ok": True,
            "order_id": str(payload["order_id"]),
            "source_order_id": str(payload["source_order_id"]),
            "payment_url": links["payment_url"],
            "donatepay_payment_url": links["donatepay_payment_url"],
            "cryptobot_payment_url": links["cryptobot_payment_url"],
            "lzt_payment_url": links["lzt_payment_url"],
            "secondary_payment_url": links["secondary_payment_url"],
            "cryptobot_payment_label": payment_provider_label(CRYPTOBOT_PROVIDER),
            "lzt_payment_label": payment_provider_label(LZT_PROVIDER),
            "secondary_payment_label": SECONDARY_PAYMENT_LABEL,
            "payment_provider": str(payload["provider"]),
            "payment_provider_label": str(payload["provider_label"]),
            "plan_replaced": bool(payload.get("plan_replaced")),
            "plan": plan_data,
            "expires_at": payload.get("expires_at"),
        }
    )


async def webapp_admin_pricing_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    pricing = get_editable_pricing_config()
    plans = get_active_plans()
    return web.json_response(
        {
            "ok": True,
            "pricing": pricing,
            "preview_plans": [serialize_plan(plan) for plan in plans.values()],
            "sale_text": get_sale_text(),
        }
    )


async def webapp_admin_save_pricing_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    normalized, normalize_reason = normalize_pricing_payload(body.get("pricing"))
    if not normalized:
        return webapp_error(normalize_reason, 400)

    save_pricing_config(normalized)
    plans = get_active_plans()
    print(f"[admin] Pricing updated from webapp by {telegram_id}")
    return web.json_response(
        {
            "ok": True,
            "pricing": normalized,
            "preview_plans": [serialize_plan(plan) for plan in plans.values()],
            "sale_text": get_sale_text(),
        }
    )


async def webapp_admin_notify_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    text = str(body.get("text") or "").strip()
    if not text:
        text = get_sale_text() or ""
    if not text:
        return webapp_error("empty_notify_text", 400)

    sent, failed = await broadcast_text(text)
    print(f"[admin] Broadcast from webapp by {telegram_id}: sent={sent} failed={failed}")
    return web.json_response({"ok": True, "sent": sent, "failed": failed})


def webapp_admin_runtime_state_payload() -> dict[str, Any]:
    return {
        "update_notify_mode": get_update_notify_mode(),
        "maintenance_mode": maintenance_mode_enabled(),
    }


async def webapp_admin_runtime_state_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    return web.json_response({"ok": True, **webapp_admin_runtime_state_payload()})


async def webapp_admin_set_update_mode_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    mode = str(body.get("mode") or "").strip().lower()
    if mode not in {"auto", "manual"}:
        return webapp_error("bad_mode", 400)

    set_update_notify_manual_mode(mode == "manual")
    print(f"[admin] update_notify_mode set from webapp by {telegram_id}: {mode}")
    return web.json_response({"ok": True, **webapp_admin_runtime_state_payload()})


async def webapp_admin_send_update_notice_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    sent, failed = await send_update_notice_broadcast()
    print(f"[admin] update_notice from webapp by {telegram_id}: sent={sent} failed={failed}")
    return web.json_response(
        {
            "ok": True,
            "sent": sent,
            "failed": failed,
            **webapp_admin_runtime_state_payload(),
        }
    )


async def webapp_admin_set_maintenance_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    enabled_raw = body.get("enabled")
    enabled = _meta_to_bool(enabled_raw, default=False)
    previous = maintenance_mode_enabled()
    changed = enabled != previous
    sent, failed = 0, 0

    if changed:
        set_maintenance_mode(enabled)
        sent, failed = await broadcast_text(maintenance_broadcast_text(enabled))
        print(
            f"[admin] maintenance_mode set from webapp by {telegram_id}: "
            f"{enabled} sent={sent} failed={failed}"
        )

    return web.json_response(
        {
            "ok": True,
            "changed": changed,
            "sent": sent,
            "failed": failed,
            **webapp_admin_runtime_state_payload(),
        }
    )


async def webapp_admin_promocodes_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    return web.json_response({"ok": True, "promocodes": get_promocodes_for_admin()})


async def webapp_admin_create_promocode_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    code = str(body.get("code") or "")
    discount_rub = _safe_int(body.get("discount_rub"), 0)
    expires_at = str(body.get("expires_at") or "")
    max_activations = _safe_int(body.get("max_activations"), 0)

    created, create_reason = create_or_update_promocode(
        code=code,
        discount_rub=discount_rub,
        expires_at=expires_at,
        max_activations=max_activations,
        created_by=telegram_id,
    )
    if not created:
        return webapp_error(create_reason, 400)

    return web.json_response({"ok": True, "promocodes": get_promocodes_for_admin()})


def serialize_suspicious_flag(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "telegram_id": int(row["telegram_id"]),
        "flag_type": str(row["flag_type"] or ""),
        "details": str(row["details"] or ""),
        "created_at": str(row["created_at"] or ""),
        "last_seen_at": str(row["last_seen_at"] or ""),
        "seen_count": int(row["seen_count"] or 1),
        "resolved": int(row["resolved"] or 0) == 1,
    }


async def webapp_admin_flags_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    limit = max(1, min(_safe_int(body.get("limit"), 30), 200))
    unresolved_only = _meta_to_bool(body.get("unresolved_only"), default=True)
    rows = get_recent_suspicious_flags(limit=limit, unresolved_only=unresolved_only)
    return web.json_response(
        {
            "ok": True,
            "flags": [serialize_suspicious_flag(row) for row in rows],
            "unresolved_only": unresolved_only,
        }
    )


async def webapp_admin_resolve_flag_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, telegram_id, reason, status = validate_webapp_admin_init_data(
        str(body.get("init_data") or "")
    )
    if not ok:
        return webapp_error(reason, status)

    flag_id = _safe_int(body.get("flag_id"), 0)
    if flag_id <= 0:
        return webapp_error("bad_flag_id", 400)

    changed = resolve_suspicious_flag(flag_id, resolved_by=telegram_id)
    if not changed:
        return webapp_error("flag_not_found_or_resolved", 404)

    rows = get_recent_suspicious_flags(limit=30, unresolved_only=True)
    return web.json_response(
        {
            "ok": True,
            "resolved_flag_id": flag_id,
            "flags": [serialize_suspicious_flag(row) for row in rows],
        }
    )


async def webapp_admin_find_user_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    query = str(body.get("query") or "").strip()
    if not query:
        return webapp_error("missing_query", 400)

    users = admin_search_users(query=query, limit=30)
    return web.json_response({"ok": True, "users": users})


async def webapp_admin_grant_subscription_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    telegram_id_raw = str(body.get("telegram_id") or "").strip()
    if not telegram_id_raw.isdigit():
        return webapp_error("bad_telegram_id", 400)
    days = _safe_int(body.get("days"), 0)
    if days <= 0:
        return webapp_error("bad_days", 400)

    user, grant_reason = await admin_grant_subscription_days(int(telegram_id_raw), days)
    if not user:
        return webapp_error(grant_reason, 404)
    return web.json_response({"ok": True, "user": user, "reason": grant_reason})


async def webapp_admin_remove_subscription_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    ok, _, reason, status = validate_webapp_admin_init_data(str(body.get("init_data") or ""))
    if not ok:
        return webapp_error(reason, status)

    telegram_id_raw = str(body.get("telegram_id") or "").strip()
    if not telegram_id_raw.isdigit():
        return webapp_error("bad_telegram_id", 400)

    user, remove_reason = await admin_remove_subscription(int(telegram_id_raw))
    if not user:
        return webapp_error(remove_reason, 404)
    return web.json_response({"ok": True, "user": user, "reason": remove_reason})


def make_web_app() -> web.Application:
    app = web.Application()

    donatepay_webhook_path = _normalize_http_path(DONATEPAY_WEBHOOK_PATH, "/donatepay/webhook")
    cryptobot_webhook_path = _normalize_http_path(CRYPTOBOT_WEBHOOK_PATH, "/cryptobot/webhook")
    lzt_webhook_path = _normalize_http_path(LZT_WEBHOOK_PATH, "/lzt/webhook")
    subscription_path = _normalize_http_path(SUBSCRIPTION_PATH, "/sub").rstrip("/")
    if not subscription_path:
        subscription_path = "/sub"

    app.router.add_post(donatepay_webhook_path, donatepay_webhook)
    app.router.add_post(cryptobot_webhook_path, cryptobot_webhook)
    app.router.add_post(lzt_webhook_path, lzt_webhook)
    app.router.add_get(f"{subscription_path}/{{sub_token}}/profile", subscription_profile_page)
    app.router.add_get(f"{subscription_path}/{{sub_token}}", subscription_feed)
    app.router.add_get(f"{subscription_path}/{{telegram_id}}/{{token}}/profile", subscription_profile_page)
    app.router.add_get(f"{subscription_path}/{{telegram_id}}/{{token}}", subscription_feed)
    app.router.add_get("/health", healthcheck)
    app.router.add_get("/status", public_status_page)
    app.router.add_get("/status.json", public_status_json)
    app.router.add_get("/pay/success", payment_success_page)
    app.router.add_get("/pay/success/", payment_success_page)
    app.router.add_get("/webapp", webapp_page)
    app.router.add_get("/webapp/", webapp_page)
    app.router.add_get("/webapp/api/plans", webapp_plans_api)
    app.router.add_post("/webapp/api/me", webapp_me_api)
    app.router.add_post("/webapp/api/create-order", webapp_create_order_api)
    app.router.add_post("/webapp/api/order-status", webapp_order_status_api)
    app.router.add_post("/webapp/api/activate-promocode", webapp_activate_promocode_api)
    app.router.add_post("/webapp/api/cancel-order", webapp_cancel_order_api)
    app.router.add_post("/webapp/api/repeat-order", webapp_repeat_order_api)
    app.router.add_post("/webapp/api/admin/pricing", webapp_admin_pricing_api)
    app.router.add_post("/webapp/api/admin/save-pricing", webapp_admin_save_pricing_api)
    app.router.add_post("/webapp/api/admin/notify", webapp_admin_notify_api)
    app.router.add_post("/webapp/api/admin/runtime-state", webapp_admin_runtime_state_api)
    app.router.add_post("/webapp/api/admin/set-update-mode", webapp_admin_set_update_mode_api)
    app.router.add_post("/webapp/api/admin/send-update-notice", webapp_admin_send_update_notice_api)
    app.router.add_post("/webapp/api/admin/set-maintenance", webapp_admin_set_maintenance_api)
    app.router.add_post("/webapp/api/admin/promocodes", webapp_admin_promocodes_api)
    app.router.add_post("/webapp/api/admin/create-promocode", webapp_admin_create_promocode_api)
    app.router.add_post("/webapp/api/admin/flags", webapp_admin_flags_api)
    app.router.add_post("/webapp/api/admin/resolve-flag", webapp_admin_resolve_flag_api)
    app.router.add_post("/webapp/api/admin/find-user", webapp_admin_find_user_api)
    app.router.add_post("/webapp/api/admin/grant-subscription", webapp_admin_grant_subscription_api)
    app.router.add_post("/webapp/api/admin/remove-subscription", webapp_admin_remove_subscription_api)
    return app


@dp.message(Command("start"))
async def start_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)
    start_arg = command_args(message)
    start_token = start_arg.split(maxsplit=1)[0].strip() if start_arg else ""
    referral_notice = ""

    referrer_id, referral_code = parse_referral_start_payload(start_token)
    if referrer_id:
        linked, reason = link_referral_if_possible(
            invited_telegram_id=message.from_user.id,
            invited_username=message.from_user.username,
            referrer_telegram_id=referrer_id,
            referral_code=referral_code,
        )
        if linked:
            referral_notice = (
                "\n\n🎯 Реферальная ссылка применена.\n"
                f"Если вы оплатите тариф от {REFERRAL_MIN_PLAN_DAYS} дней, "
                f"пригласивший вас получит +{REFERRAL_REWARD_DAYS} {day_word(REFERRAL_REWARD_DAYS)}."
            )
        elif reason == "already_paid":
            referral_notice = "\n\nℹ️ Реферальная привязка доступна только до первой оплаты."
        elif reason == "self_referral":
            referral_notice = "\n\nℹ️ Свою реферальную ссылку применить нельзя."

    order_from_start = parse_start_order_id(start_token)
    if order_from_start:
        payment = get_payment(order_from_start, apply_expiry=True)
        if payment and int(payment["telegram_id"] or 0) == int(message.from_user.id):
            if str(payment["status"] or "").strip().lower() == "pending":
                try:
                    await sync_pending_payment_order(order_from_start)
                except Exception as exc:  # noqa: BLE001
                    print(f"[start] Sync error for {order_from_start}: {exc}")
                payment = get_payment(order_from_start, apply_expiry=True)

            if payment and str(payment["status"] or "").strip().lower() == "paid":
                user = get_user(message.from_user.id)
                expiry = str(user["subscription_end"] or "-") if user else "-"
                await message.answer(
                    "✅ Оплата подтверждена.\n"
                    f"Заказ: {order_from_start}\n"
                    f"Подписка активна до: {expiry}",
                    reply_markup=build_order_closed_keyboard(order_from_start),
                )
            elif payment and str(payment["status"] or "").strip().lower() == "pending":
                amount_rub = int(round(float(payment["amount_rub"] or 0)))
                links = await build_payment_links_for_order(
                    order_from_start,
                    amount_rub,
                    str(payment["provider"] or ""),
                    telegram_id=message.from_user.id,
                    username=message.from_user.username,
                )
                expires_at = payment_expires_at_str(str(payment["created_at"] or "")) or "-"
                await message.answer(
                    "⏳ Заказ пока не оплачен.\n"
                    f"Заказ: {order_from_start}\n"
                    f"Сумма: {amount_rub} ₽\n"
                    f"Истекает: {expires_at}",
                    reply_markup=build_payment_keyboard(
                        links["payment_url"],
                        order_from_start,
                        str(payment["provider"] or ""),
                        donatepay_url=links["donatepay_payment_url"],
                        cryptobot_url=links["cryptobot_payment_url"],
                        lzt_url=links["lzt_payment_url"],
                        secondary_payment_url=links["secondary_payment_url"],
                    ),
                )
            elif payment and str(payment["status"] or "").strip().lower() == "cancelled":
                await message.answer(
                    "ℹ️ Этот заказ уже закрыт (отменен или истек).\n"
                    "Нажмите «Повторить заказ», чтобы создать новый.",
                    reply_markup=build_order_closed_keyboard(order_from_start),
                )

    features = [
        "• Покупка и продление подписки",
        "• Моментальная выдача ключа",
        "• Инструкции для Android / iOS / Windows / macOS / Linux",
        "• Поддержка через Telegram",
    ]
    if WEBAPP_PUBLIC_URL:
        features.append("• Telegram Mini App для оплаты и проверки статуса")

    intro = (
        "⚡ BoxVolt VPN\n"
        "Стабильный VLESS Reality, быстрый доступ и простое подключение.\n\n"
        "Что умеет бот:\n"
        f"{chr(10).join(features)}"
        f"{referral_notice}"
    )

    main_keyboard = build_main_keyboard(message.from_user.id)
    if START_PHOTO:
        try:
            photo_value = START_PHOTO
            local_photo = Path(photo_value)
            if not local_photo.is_absolute():
                local_photo = BASE_DIR / photo_value
            photo_input: str | FSInputFile = (
                FSInputFile(str(local_photo))
                if local_photo.exists() and local_photo.is_file()
                else photo_value
            )

            if len(intro) <= 1024:
                await message.answer_photo(
                    photo_input,
                    caption=intro,
                    reply_markup=main_keyboard,
                )
                return

            await message.answer_photo(photo_input)
        except Exception as exc:  # noqa: BLE001
            print(f"[start-photo] Failed to send start photo: {exc}")

    await message.answer(intro, reply_markup=main_keyboard)


@dp.message(Command("prices"))
async def prices_handler(message: Message) -> None:
    plans = get_active_plans()
    lines = ["💳 Актуальные тарифы:"]
    for plan in plans.values():
        lines.append(f"• {plan_line(plan)}")

    sale_text = get_sale_text()
    if sale_text:
        lines.append("")
        lines.append(sale_text)

    await message.answer("\n".join(lines))


@dp.message(Command("myid"))
async def myid_handler(message: Message) -> None:
    lines = [f"Ваш Telegram ID: <code>{message.from_user.id}</code>"]
    if message.chat and message.chat.id != message.from_user.id:
        lines.append(f"Chat ID: <code>{message.chat.id}</code>")
    topic_id = getattr(message, "message_thread_id", None)
    if topic_id:
        lines.append(f"Topic ID: <code>{topic_id}</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("rules"))
async def rules_command_handler(message: Message) -> None:
    await message.answer(build_rules_text(), reply_markup=build_main_keyboard(message.from_user.id))


@dp.message(F.text == "📜 Правила")
async def rules_text_handler(message: Message) -> None:
    await rules_command_handler(message)


async def activate_promocode_for_message(message: Message, raw_code: str) -> tuple[bool, str]:
    code = normalize_promo_code(raw_code)
    if not code:
        await message.answer("Формат: `/promo CODE`", parse_mode="Markdown")
        return False, "invalid_code"

    upsert_user(message.from_user.id, message.from_user.username)
    activated, reason, active = activate_promocode_for_user(message.from_user.id, code)
    if not activated:
        await message.answer(f"❌ {promo_error_text(reason)}")
        return False, reason

    if not active:
        await message.answer("✅ Промокод активирован.")
        return True, "activated"

    await message.answer(
        "✅ Промокод активирован.\n"
        f"Код: {active['code']}\n"
        f"Скидка: -{active['discount_rub']} ₽\n"
        f"Действует до: {active['expires_at']}\n\n"
        "Скидка применится автоматически при создании следующего заказа."
    )
    return True, "activated"


@dp.message(Command("promo"))
async def promo_activate_command_handler(message: Message) -> None:
    await activate_promocode_for_message(message, command_args(message))


@dp.message(Command("sale_notify"))
async def sale_notify_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return

    text = command_args(message) or get_sale_text()
    if not text:
        await message.answer(
            "⚠️ Текст акции пуст.\n"
            "Укажите текст после команды `/sale_notify ваш текст` "
            "или заполните `sale_message` в pricing.json."
        )
        return

    await message.answer("📣 Запускаю рассылку акции...")
    sent, failed = await broadcast_text(text)
    await message.answer(f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}")


async def show_admin_panel(message: Message) -> None:
    await message.answer(
        format_admin_panel_text(),
        reply_markup=build_admin_panel_keyboard(),
    )


@dp.message(Command("admin"))
async def admin_panel_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await show_admin_panel(message)


@dp.message(Command("admin_stats"))
async def admin_stats_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await message.answer(format_admin_stats_text())


@dp.message(Command("blacklist_add"))
async def blacklist_add_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    args = command_args(message)
    if not args:
        await message.answer("Формат: /blacklist_add <telegram_id> [причина]")
        return
    parts = args.split(maxsplit=1)
    telegram_raw = parts[0].strip()
    if not re.fullmatch(r"-?\d+", telegram_raw):
        await message.answer("Некорректный telegram_id")
        return
    target_id = int(telegram_raw)
    reason = parts[1].strip() if len(parts) > 1 else ""
    blacklist_add_user(target_id, reason=reason, created_by=message.from_user.id)
    reason_tail = f"\nПричина: {reason}" if reason else ""
    await message.answer(f"✅ Пользователь {target_id} добавлен в blacklist.{reason_tail}")


@dp.message(Command("blacklist_del"))
async def blacklist_del_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    telegram_raw = command_args(message).strip()
    if not re.fullmatch(r"-?\d+", telegram_raw):
        await message.answer("Формат: /blacklist_del <telegram_id>")
        return
    target_id = int(telegram_raw)
    deleted = blacklist_remove_user(target_id)
    if deleted > 0:
        await message.answer(f"✅ Пользователь {target_id} удален из blacklist.")
    else:
        await message.answer(f"ℹ️ Пользователь {target_id} не найден в blacklist таблице.")


@dp.message(Command("blacklist_list"))
async def blacklist_list_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    rows = blacklist_list(limit=100)
    lines = ["⛔ Blacklist (DB):"]
    if not rows:
        lines.append("• пусто")
    else:
        for row in rows[:50]:
            reason = str(row["reason"] or "").strip() or "без причины"
            lines.append(f"• {int(row['telegram_id'])} — {reason}")
    if BLACKLIST_TELEGRAM_IDS:
        env_values = ", ".join(str(x) for x in sorted(BLACKLIST_TELEGRAM_IDS))
        lines.append("")
        lines.append(f"ENV blacklist: {env_values}")
    await message.answer("\n".join(lines))


@dp.message(Command("flags_list"))
async def flags_list_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    rows = get_recent_suspicious_flags(limit=40, unresolved_only=True)
    lines = ["🚩 Подозрительные флаги (unresolved):"]
    if not rows:
        lines.append("• пусто")
        await message.answer("\n".join(lines))
        return

    for row in rows:
        details = str(row["details"] or "").strip()
        details_tail = f" | {details}" if details else ""
        lines.append(
            f"• #{int(row['id'])} tg:{int(row['telegram_id'])} {str(row['flag_type'])}"
            f" x{int(row['seen_count'] or 1)}"
            f" | last:{row['last_seen_at']}{details_tail}"
        )
    await message.answer("\n".join(lines))


@dp.message(Command("flags_resolve"))
async def flags_resolve_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    raw = command_args(message).strip()
    if not raw.isdigit():
        await message.answer("Формат: /flags_resolve <id>")
        return
    ok = resolve_suspicious_flag(int(raw), resolved_by=message.from_user.id)
    if ok:
        await message.answer(f"✅ Флаг #{raw} помечен как обработанный.")
    else:
        await message.answer(f"ℹ️ Флаг #{raw} не найден или уже обработан.")


@dp.message(Command("backup_now"))
async def backup_now_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await message.answer("🗄 Запускаю резервное копирование...")
    ok, info = await maybe_run_auto_backup(force=True)
    if ok:
        await message.answer(f"✅ Бэкап создан: {info}")
    else:
        await message.answer(f"⚠️ Не удалось создать бэкап: {info}")


@dp.message(Command("webapp"))
async def open_webapp_handler(message: Message) -> None:
    if not WEBAPP_PUBLIC_URL:
        await message.answer("❌ WebApp не настроен. Укажите `WEBAPP_PUBLIC_URL` в .env.")
        return

    keyboard = build_webapp_open_keyboard("plans")
    if keyboard:
        await message.answer(
            "🧩 Нажмите кнопку ниже, чтобы открыть Mini App внутри Telegram.",
            reply_markup=keyboard,
        )
        return
    await message.answer(f"🧩 Откройте Mini App: {WEBAPP_PUBLIC_URL}")


@dp.message(F.text == "🧩 Mini App")
async def mini_app_text_handler(message: Message) -> None:
    if not WEBAPP_PUBLIC_URL:
        await message.answer("❌ WebApp не настроен. Укажите `WEBAPP_PUBLIC_URL` в .env.")
        return

    keyboard = build_webapp_open_keyboard("plans")
    if keyboard:
        await message.answer(
            "🧩 Откройте Mini App кнопкой ниже (так корректно передаётся initData).",
            reply_markup=keyboard,
        )
        return
    await message.answer(f"🧩 Ссылка на Mini App: {WEBAPP_PUBLIC_URL}")


@dp.message(Command("buy"))
async def buy_menu_command_handler(message: Message) -> None:
    await buy_menu_handler(message)


@dp.message(F.text == "💳 Купить подписку")
async def buy_menu_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)

    active_provider = get_active_payment_provider()
    if not payment_provider_is_ready(active_provider):
        missing = "`DONATEPAY_DONATE_BASE_URL` и `DONATEPAY_API_KEY`"
        if active_provider == CRYPTOBOT_PROVIDER:
            missing = "`CRYPTOBOT_ENABLED=1` и `CRYPTOBOT_API_TOKEN`"
        elif active_provider == LZT_PROVIDER:
            missing = "`LZT_ENABLED=1`, `LZT_API_TOKEN`, `LZT_MERCHANT_ID`"
        elif active_provider == SECONDARY_PROVIDER:
            missing = "`SECONDARY_PAYMENT_ENABLED=1` и `SECONDARY_PAYMENT_URL`"
        await message.answer(
            f"⚠️ Оплата через {payment_provider_label(active_provider)} временно недоступна: "
            f"проверьте {missing} в `.env`."
        )
        return

    plans = get_active_plans()
    pending = get_latest_pending_payment(message.from_user.id)
    pending_text = ""
    if pending:
        expires_at = payment_expires_at_str(pending["created_at"]) or "-"
        promo_tail = ""
        if str(pending["promo_code"] or "").strip() and int(pending["promo_discount_rub"] or 0) > 0:
            promo_tail = (
                f" (промокод {pending['promo_code']}, -{int(pending['promo_discount_rub'])} ₽)"
            )
        pending_text = (
            "\n\nНайден незавершенный заказ:\n"
            f"Код: {pending['order_id']}\n"
            f"Сумма: {pending['amount_rub']} ₽{promo_tail}\n"
            f"Истекает: {expires_at}\n"
            "Можно оплатить его или создать новый."
        )

    sale_text = get_sale_text()
    sale_block = f"\n\n{sale_text}" if sale_text else ""
    text = (
        "💳 Выберите тариф.\n"
        "После выбора бот создаст код заказа и откроет оплату."
        f"\nНеоплаченные заказы автоматически отменяются через {PAYMENT_PENDING_TTL_MINUTES} минут."
        + sale_block
        + pending_text
    )
    await message.answer(text, reply_markup=build_plan_keyboard(plans))


@dp.callback_query(F.data.startswith("buy:"))
async def buy_plan_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return

    active_provider = get_active_payment_provider()
    if not payment_provider_is_ready(active_provider):
        await callback.answer("Оплата не настроена", show_alert=True)
        return

    plan_code = callback.data.split(":", maxsplit=1)[1]
    plans = get_active_plans()
    plan = plans.get(plan_code)
    if not plan:
        await callback.answer("Неизвестный тариф", show_alert=True)
        return

    upsert_user(callback.from_user.id, callback.from_user.username)
    if not is_admin_user(callback.from_user.id) and is_user_blacklisted(callback.from_user.id):
        await callback.answer("⛔ Доступ ограничен", show_alert=True)
        return

    retry_after, retry_reason = order_create_retry_state(callback.from_user.id)
    if retry_after > 0:
        log_suspicious_flag(
            callback.from_user.id,
            "order_rate_limited",
            f"context=buy_callback reason={retry_reason} retry_after={retry_after}",
        )
        await callback.answer(
            f"Слишком часто. Повторите через {retry_after} сек.",
            show_alert=True,
        )
        return

    final_amount, promo = calculate_plan_price_for_user(callback.from_user.id, plan)
    order_id = create_payment_order(
        telegram_id=callback.from_user.id,
        plan=plan,
        amount_rub=final_amount,
        promo_code=promo["code"] if promo else None,
        promo_discount_rub=promo["discount_rub"] if promo else 0,
    )
    payment = get_payment(order_id, apply_expiry=False)
    provider = payment["provider"] if payment else active_provider
    links = await build_payment_links_for_order(
        order_id,
        final_amount,
        provider,
        telegram_id=callback.from_user.id,
        username=callback.from_user.username,
    )

    amount_line = f"Сумма: {final_amount} ₽"
    details: list[str] = []
    if plan.discount_percent > 0 and plan.base_amount_rub > plan.amount_rub:
        details.append(f"Скидка тарифа: -{plan.discount_percent}% (было {plan.base_amount_rub} ₽)")
    if promo:
        details.append(f"Промокод {promo['code']}: -{promo['discount_rub']} ₽")
    if details:
        amount_line += "\n" + "\n".join(details)

    provider_label = payment_provider_label(provider)
    pay_step_line = f"1. Нажмите «Оплатить в {provider_label}».\n"
    if WEBAPP_PUBLIC_URL and (
        links["donatepay_payment_url"] or links["cryptobot_payment_url"] or links["lzt_payment_url"]
    ):
        pay_step_line = (
            "1. Нажмите «Выбрать оплату в Mini App» (выбор DonatePay/CryptoBot/LZT).\n"
        )
    elif links["payment_url"]:
        pay_step_line = f"1. Нажмите «Оплатить в {provider_label}».\n"

    text = (
        f"🧾 Заказ создан: {order_id}\n"
        f"Тариф: {plan.title}\n"
        f"{amount_line}\n\n"
        "Как оплатить:\n"
        f"{pay_step_line}"
        f"2. В {provider_label} укажите сумму не меньше: {final_amount} ₽\n"
        "   Комиссия сверху допустима.\n"
        f"3. Если есть поле комментария, вставьте код: {order_id}\n"
        "4. После оплаты нажмите «Проверить оплату».\n\n"
        f"⏱ Неоплаченный заказ отменится автоматически через {PAYMENT_PENDING_TTL_MINUTES} минут."
    )
    if links["cryptobot_payment_url"] and normalize_payment_provider(provider) != CRYPTOBOT_PROVIDER:
        text += "\n\n₿ Дополнительно доступна оплата через CryptoBot."
    if links["lzt_payment_url"] and normalize_payment_provider(provider) != LZT_PROVIDER:
        text += "\n\n⚡ Дополнительно доступна оплата через LZT Market."
    if SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL:
        text += f"\n\n🪙 Резервный способ доступен кнопкой «{SECONDARY_PAYMENT_LABEL}»."
    text = apply_order_status_to_text(text, "⏳ Ожидаем оплату.")

    await callback.message.answer(
        text,
        reply_markup=build_payment_keyboard(
            links["payment_url"],
            order_id,
            provider,
            donatepay_url=links["donatepay_payment_url"],
            cryptobot_url=links["cryptobot_payment_url"],
            lzt_url=links["lzt_payment_url"],
            secondary_payment_url=links["secondary_payment_url"],
        ),
    )
    await callback.answer("Платеж создан")


@dp.callback_query(F.data.startswith("paycheck:"))
async def payment_check_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return

    order_id = callback.data.split(":", maxsplit=1)[1]
    payment = get_payment(order_id)

    if not payment or int(payment["telegram_id"]) != callback.from_user.id:
        await callback.answer("Заказ не найден", show_alert=True)
        return

    if payment["status"] == "pending":
        try:
            await sync_pending_payment_order(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[paycheck] Sync error for {order_id}: {exc}")
        payment = get_payment(order_id)
        if not payment:
            await callback.answer("Заказ не найден", show_alert=True)
            return

    status_text = ""
    keyboard: InlineKeyboardMarkup | None = None
    answer_text = "Статус обновлен"

    if payment["status"] == "paid":
        user = get_user(callback.from_user.id)
        expiry = user["subscription_end"] if user else "-"
        status_text = f"✅ Оплата подтверждена. Подписка активна до: {expiry}."
        keyboard = build_order_closed_keyboard(order_id)
        answer_text = "Оплата подтверждена"
    elif payment["status"] == "cancelled":
        status_text = (
            "❌ Заказ отменен или просрочен.\n"
            "Нажмите «Повторить заказ», чтобы быстро создать новый."
        )
        keyboard = build_order_closed_keyboard(order_id)
        answer_text = "Заказ отменен"
    else:
        expires_at = payment_expires_at_str(payment["created_at"]) or "-"
        status_text = (
            "⏳ Платеж еще не подтвержден.\n"
            f"Заказ автоматически отменится в: {expires_at}."
        )
        amount_rub = int(round(float(payment["amount_rub"])))
        links = await build_payment_links_for_order(
            order_id,
            amount_rub,
            payment["provider"],
            telegram_id=callback.from_user.id,
            username=callback.from_user.username,
        )
        keyboard = build_payment_keyboard(
            links["payment_url"],
            order_id,
            payment["provider"],
            donatepay_url=links["donatepay_payment_url"],
            cryptobot_url=links["cryptobot_payment_url"],
            lzt_url=links["lzt_payment_url"],
            secondary_payment_url=links["secondary_payment_url"],
        )
        answer_text = "Пока не оплачено"

    updated_text = apply_order_status_to_text(callback.message.text or "", status_text)
    try:
        await callback.message.edit_text(updated_text, reply_markup=keyboard)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            print(f"[paycheck] Failed to edit message for {order_id}: {exc}")

    await callback.answer(answer_text, show_alert=False)


@dp.callback_query(F.data.startswith("paycancel:"))
async def payment_cancel_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return

    order_id = callback.data.split(":", maxsplit=1)[1]
    ok, reason = cancel_payment_order(order_id, callback.from_user.id)
    if ok:
        updated_text = apply_order_status_to_text(
            callback.message.text or "",
            "❌ Заказ отменен. Вы можете создать новый тариф в любое время.",
        )
        try:
            await callback.message.edit_text(
                updated_text,
                reply_markup=build_order_closed_keyboard(order_id),
            )
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                print(f"[paycancel] Failed to edit message for {order_id}: {exc}")
        await callback.answer("Заказ отменен", show_alert=False)
        return

    if reason == "order_not_found":
        await callback.answer("Заказ не найден", show_alert=True)
        return

    if reason.startswith("not_pending:"):
        status = reason.split(":", maxsplit=1)[1]
        if status == "paid":
            await callback.answer("Заказ уже оплачен", show_alert=False)
        else:
            await callback.answer("Заказ уже отменен", show_alert=False)
        return

    await callback.answer("Не удалось отменить заказ", show_alert=True)


@dp.callback_query(F.data.startswith("payrepeat:"))
async def payment_repeat_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return

    source_order_id = callback.data.split(":", maxsplit=1)[1].strip().upper()
    if not source_order_id:
        await callback.answer("Заказ не найден", show_alert=True)
        return

    if not payment_provider_is_ready(get_active_payment_provider()):
        await callback.answer("Оплата сейчас недоступна", show_alert=True)
        return

    retry_after, retry_reason = order_create_retry_state(callback.from_user.id)
    if retry_after > 0:
        log_suspicious_flag(
            callback.from_user.id,
            "order_rate_limited",
            f"context=payrepeat reason={retry_reason} retry_after={retry_after}",
        )
        await callback.answer(
            f"Слишком часто. Повторите через {retry_after} сек.",
            show_alert=True,
        )
        return

    ok, reason, payload = recreate_payment_order_from_previous(
        source_order_id,
        callback.from_user.id,
        username=callback.from_user.username,
    )
    if not ok or not payload:
        reason_map = {
            "order_not_found": "Исходный заказ не найден",
            "forbidden": "Это не ваш заказ",
            "order_not_recreatable": "Этот заказ нельзя повторить",
            "plan_not_found": "Тариф для повтора не найден",
        }
        await callback.answer(reason_map.get(reason, "Не удалось повторить заказ"), show_alert=True)
        return

    plan = payload["plan"]
    promo = payload.get("promo")
    details: list[str] = []
    if plan.discount_percent > 0 and plan.base_amount_rub > plan.amount_rub:
        details.append(f"Скидка тарифа: -{plan.discount_percent}% (было {plan.base_amount_rub} ₽)")
    if isinstance(promo, dict):
        details.append(f"Промокод {promo.get('code')}: -{promo.get('discount_rub')} ₽")
    details_block = f"\n{chr(10).join(details)}" if details else ""

    new_order_id = str(payload["order_id"])
    amount_rub = int(payload["amount_rub"])
    provider_label = str(payload["provider_label"])
    provider_code = str(payload["provider"])
    links = await build_payment_links_for_order(
        new_order_id,
        amount_rub,
        provider_code,
        telegram_id=callback.from_user.id,
        username=callback.from_user.username,
    )
    text = (
        f"🔁 Создан новый заказ: {new_order_id}\n"
        f"Тариф: {plan.title}\n"
        f"Сумма: {amount_rub} ₽{details_block}\n\n"
        "Как оплатить:\n"
        "1. Нажмите кнопку оплаты ниже.\n"
        f"2. В {provider_label} укажите сумму не меньше: {amount_rub} ₽\n"
        "   Комиссия сверху допустима.\n"
        f"3. Если есть поле комментария, вставьте код: {new_order_id}\n"
        "4. После оплаты нажмите «Проверить оплату».\n\n"
        f"⏱ Неоплаченный заказ отменится автоматически через {PAYMENT_PENDING_TTL_MINUTES} минут."
    )
    if bool(payload.get("plan_replaced")):
        text = (
            f"ℹ️ Исходный тариф больше недоступен, подобран ближайший: {plan.title}.\n\n"
            f"{text}"
        )
    if links["cryptobot_payment_url"] and normalize_payment_provider(provider_code) != CRYPTOBOT_PROVIDER:
        text += "\n\n₿ Дополнительно доступна оплата через CryptoBot."
    if links["lzt_payment_url"] and normalize_payment_provider(provider_code) != LZT_PROVIDER:
        text += "\n\n⚡ Дополнительно доступна оплата через LZT Market."
    if SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL:
        text += f"\n\n🪙 Резервный способ доступен кнопкой «{SECONDARY_PAYMENT_LABEL}»."
    text = apply_order_status_to_text(text, "⏳ Ожидаем оплату.")

    if callback.message:
        previous_text = apply_order_status_to_text(
            callback.message.text or "",
            f"🔁 Создан новый заказ: {new_order_id}",
        )
        try:
            await callback.message.edit_text(previous_text, reply_markup=build_order_closed_keyboard(source_order_id))
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                print(f"[payrepeat] Failed to edit old message for {source_order_id}: {exc}")

    if callback.message:
        await callback.message.answer(
            text,
            reply_markup=build_payment_keyboard(
                links["payment_url"],
                new_order_id,
                provider_code,
                donatepay_url=links["donatepay_payment_url"],
                cryptobot_url=links["cryptobot_payment_url"],
                lzt_url=links["lzt_payment_url"],
                secondary_payment_url=links["secondary_payment_url"],
            ),
        )
    else:
        await bot.send_message(
            callback.from_user.id,
            text,
            reply_markup=build_payment_keyboard(
                links["payment_url"],
                new_order_id,
                provider_code,
                donatepay_url=links["donatepay_payment_url"],
                cryptobot_url=links["cryptobot_payment_url"],
                lzt_url=links["lzt_payment_url"],
                secondary_payment_url=links["secondary_payment_url"],
            ),
        )
    await callback.answer("Новый заказ создан", show_alert=False)


@dp.message(F.text == "🚀 Подключить VPN")
async def vpn_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)
    user = get_user(message.from_user.id)

    if not user or not has_active_subscription(user["subscription_end"]):
        await message.answer(
            "❌ У вас нет активной подписки. Откройте «Купить подписку».",
            reply_markup=build_main_keyboard(message.from_user.id),
        )
        return

    if maintenance_mode_enabled():
        await message.answer(
            maintenance_user_block_text(),
            reply_markup=build_main_keyboard(message.from_user.id),
        )
        return

    try:
        await ensure_vless_uuid(
            message.from_user.id,
            user["vless_uuid"],
            user["subscription_end"],
        )
    except Exception as exc:  # noqa: BLE001
        await message.answer(
            "⚠️ Подписка активна, но не удалось подготовить ключ.\n"
            f"Обратитесь в поддержку: {SUPPORT_CONTACT}\n"
            f"Техническая ошибка: {exc}"
        )
        return

    subscription_block = build_subscription_text_block(message.from_user.id)
    if not subscription_block:
        await message.answer(
            "⚠️ URL-подписка пока недоступна. Обратитесь в поддержку."
        )
        return
    await message.answer(
        "🔑 Ваш доступ:\n"
        f"{subscription_block}\n\n"
        "📚 Если нужно, откройте «Инструкции» для вашей ОС.",
        parse_mode="HTML",
        reply_markup=build_subscription_delivery_keyboard(message.from_user.id),
    )


@dp.message(F.text == "👤 Личный кабинет")
async def profile_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)
    user = get_user(message.from_user.id)

    if not user:
        await message.answer("Пользователь не найден. Нажмите /start")
        return

    subscription_active = has_active_subscription(user["subscription_end"])
    status = "активна" if subscription_active else "неактивна"
    subscription_end = user["subscription_end"] or "-"
    remaining = format_subscription_remaining(user["subscription_end"])
    user_uuid = user["vless_uuid"] or "-"
    route_mode = get_user_route_mode(message.from_user.id)
    route_mode_title = {
        "auto": "Авто",
        "main": "Основной",
        "reserve": "Резерв",
    }.get(route_mode, "Авто")
    role_line = "Роль: админ\n" if is_admin_user(message.from_user.id) else ""
    active_promo = get_user_active_promocode(message.from_user.id)
    promo_line = "Промокод: нет\n"
    if active_promo:
        promo_line = (
            f"Промокод: {active_promo['code']} (-{active_promo['discount_rub']} ₽, "
            f"до {active_promo['expires_at']})\n"
        )

    text = (
        "👤 Личный кабинет\n"
        f"Пользователь: {user_title(message)}\n"
        f"{role_line}"
        f"🖥 Сервер: {server_country_label()}\n"
        f"🛰 Режим маршрута: {route_mode_title}\n"
        f"Подписка: {status}\n"
        f"До: {subscription_end}\n"
        f"Осталось: {remaining}\n"
        "Рекомендуемое приложение: Happ\n"
        f"{promo_line}"
        f"UUID: {user_uuid}"
    )
    text += "\n\nДля активации промокода: кнопка «🎟 Ввести промокод» или команда /promo CODE"
    await message.answer(
        text,
        reply_markup=build_profile_keyboard(subscription_active),
    )


@dp.message(Command("profile"))
async def profile_command_handler(message: Message) -> None:
    await profile_handler(message)


@dp.message(Command("status"))
async def status_command_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)
    user = get_user(message.from_user.id)
    if not user:
        await message.answer("Пользователь не найден. Нажмите /start")
        return

    subscription_active = has_active_subscription(user["subscription_end"])
    status_text = "✅ Активна" if subscription_active else "❌ Неактивна"
    until_text = user["subscription_end"] or "-"
    remaining = format_subscription_remaining(user["subscription_end"])
    text = (
        "📶 Статус VPN\n"
        f"Подписка: {status_text}\n"
        f"До: {until_text}\n"
        f"Осталось: {remaining}"
    )
    await message.answer(text, reply_markup=build_main_keyboard(message.from_user.id))


@dp.message(F.text == "📚 Инструкции")
async def guides_handler(message: Message) -> None:
    await message.answer(
        "📚 Выберите вашу ОС/приложение:\n"
        "Основные клиенты: Happ и V2rayTun.",
        reply_markup=build_guides_keyboard(),
    )


@dp.callback_query(F.data == "profile:reissue")
async def profile_reissue_callback(callback: CallbackQuery) -> None:
    user = get_user(callback.from_user.id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    if not has_active_subscription(user["subscription_end"]):
        await callback.answer("Нужна активная подписка", show_alert=True)
        return

    if maintenance_mode_enabled():
        await callback.answer("Идут технические работы", show_alert=True)
        if callback.message:
            await callback.message.answer(maintenance_user_block_text())
        return

    await callback.answer("Перевыпускаю ключ...", show_alert=False)
    try:
        await reissue_vless_uuid(
            telegram_id=callback.from_user.id,
            existing_uuid=user["vless_uuid"],
            subscription_end=user["subscription_end"],
        )
        subscription_block = build_subscription_text_block(callback.from_user.id)
        if not subscription_block:
            raise RuntimeError("subscription_url_not_configured")
        await callback.message.answer(
            "🔄 Ключ перевыпущен. Старый ключ удален.\n"
            "Новый доступ:\n"
            f"{subscription_block}",
            parse_mode="HTML",
        )
    except Exception as exc:  # noqa: BLE001
        await callback.message.answer(f"⚠️ Не удалось перевыпустить ключ: {exc}")


@dp.callback_query(F.data == "profile:promo")
async def profile_promo_callback(callback: CallbackQuery) -> None:
    PROMO_WAITING_USERS.add(callback.from_user.id)
    await callback.message.answer(
        "🎟 Отправьте промокод следующим сообщением.\n"
        "Пример: `BOXVOLT30`\n"
        "Для отмены отправьте /cancel",
        parse_mode="Markdown",
    )
    await callback.answer("Жду промокод", show_alert=False)


@dp.callback_query(F.data == "profile:toggle_route")
async def profile_toggle_route_callback(callback: CallbackQuery) -> None:
    current = get_user_route_mode(callback.from_user.id)
    next_mode = "reserve" if current in {"auto", "main"} else "main"
    set_user_route_mode(callback.from_user.id, next_mode)
    title = "Резерв" if next_mode == "reserve" else "Основной"
    if callback.message:
        await callback.message.answer(
            f"🛰 Режим маршрута переключен: {title}.\n"
            "При следующей выдаче/обновлении ссылки профиль будет с новым приоритетом."
        )
    await callback.answer(f"Режим: {title}", show_alert=False)


async def create_order_from_profile_plan(
    callback: CallbackQuery,
    plan: Plan,
    source_label: str,
) -> tuple[bool, str]:
    active_provider = get_active_payment_provider()
    if not payment_provider_is_ready(active_provider):
        return False, "Оплата не настроена"

    if not is_admin_user(callback.from_user.id) and is_user_blacklisted(callback.from_user.id):
        return False, "⛔ Доступ ограничен"

    upsert_user(callback.from_user.id, callback.from_user.username)
    retry_after, retry_reason = order_create_retry_state(callback.from_user.id)
    if retry_after > 0:
        log_suspicious_flag(
            callback.from_user.id,
            "order_rate_limited",
            f"context=profile_renew reason={retry_reason} retry_after={retry_after}",
        )
        return False, f"Слишком часто. Повторите через {retry_after} сек."

    final_amount, promo = calculate_plan_price_for_user(callback.from_user.id, plan)
    order_id = create_payment_order(
        telegram_id=callback.from_user.id,
        plan=plan,
        amount_rub=final_amount,
        promo_code=promo["code"] if promo else None,
        promo_discount_rub=promo["discount_rub"] if promo else 0,
    )
    payment = get_payment(order_id, apply_expiry=False)
    provider = payment["provider"] if payment else active_provider
    links = await build_payment_links_for_order(
        order_id,
        final_amount,
        provider,
        telegram_id=callback.from_user.id,
        username=callback.from_user.username,
    )

    details: list[str] = []
    if plan.discount_percent > 0 and plan.base_amount_rub > plan.amount_rub:
        details.append(
            f"Скидка тарифа: -{plan.discount_percent}% (было {plan.base_amount_rub} ₽)"
        )
    if promo:
        details.append(f"Промокод {promo['code']}: -{promo['discount_rub']} ₽")
    details_block = f"\n{chr(10).join(details)}" if details else ""

    provider_label = payment_provider_label(provider)
    text = (
        f"🧾 Заказ на продление создан: {order_id}\n"
        f"Тариф: {plan.title} ({source_label})\n"
        f"Сумма: {final_amount} ₽{details_block}\n\n"
        "Как оплатить:\n"
        "1. Нажмите кнопку оплаты ниже.\n"
        f"2. В {provider_label} укажите сумму не меньше: {final_amount} ₽\n"
        "   Комиссия сверху допустима.\n"
        f"3. Если есть поле комментария, вставьте код: {order_id}\n"
        "4. После оплаты нажмите «Проверить оплату».\n\n"
        f"⏱ Неоплаченный заказ отменится автоматически через {PAYMENT_PENDING_TTL_MINUTES} минут."
    )
    if links["cryptobot_payment_url"] and normalize_payment_provider(provider) != CRYPTOBOT_PROVIDER:
        text += "\n\n₿ Дополнительно доступна оплата через CryptoBot."
    if links["lzt_payment_url"] and normalize_payment_provider(provider) != LZT_PROVIDER:
        text += "\n\n⚡ Дополнительно доступна оплата через LZT Market."
    if SECONDARY_PAYMENT_ENABLED and SECONDARY_PAYMENT_URL:
        text += f"\n\n🪙 Резервный способ доступен кнопкой «{SECONDARY_PAYMENT_LABEL}»."
    text = apply_order_status_to_text(text, "⏳ Ожидаем оплату.")

    await callback.message.answer(
        text,
        reply_markup=build_payment_keyboard(
            links["payment_url"],
            order_id,
            provider,
            donatepay_url=links["donatepay_payment_url"],
            cryptobot_url=links["cryptobot_payment_url"],
            lzt_url=links["lzt_payment_url"],
            secondary_payment_url=links["secondary_payment_url"],
        ),
    )
    return True, "Заказ на продление создан"


@dp.callback_query(F.data == "profile:renew7")
async def profile_renew_7_callback(callback: CallbackQuery) -> None:
    plan = get_plan_by_days(7)
    if not plan:
        await callback.answer("Тариф на 7 дней не найден", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "быстрое продление")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "profile:renew14")
async def profile_renew_14_legacy_callback(callback: CallbackQuery) -> None:
    plan = get_plan_by_days(14) or get_plan_by_days(7)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "быстрое продление")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "profile:renew30")
async def profile_renew_30_callback(callback: CallbackQuery) -> None:
    plan = get_plan_by_days(30)
    if not plan:
        await callback.answer("Тариф на 30 дней не найден", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "быстрое продление")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "profile:renew90")
async def profile_renew_90_callback(callback: CallbackQuery) -> None:
    plan = get_plan_by_days(90)
    if not plan:
        await callback.answer("Тариф на 90 дней не найден", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "быстрое продление")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "profile:renew365")
async def profile_renew_365_callback(callback: CallbackQuery) -> None:
    plan = get_plan_by_days(365)
    if not plan:
        await callback.answer("Тариф на 365 дней не найден", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "быстрое продление")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "profile:renew60")
async def profile_renew_60_legacy_callback(callback: CallbackQuery) -> None:
    plan = get_plan_by_days(60) or get_plan_by_days(90)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "быстрое продление")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "profile:renew_last")
async def profile_renew_last_callback(callback: CallbackQuery) -> None:
    plan = get_last_paid_plan_for_user(callback.from_user.id)
    if not plan:
        await callback.answer("Нет оплаченных тарифов для повтора", show_alert=True)
        return
    ok, text = await create_order_from_profile_plan(callback, plan, "повтор прошлого тарифа")
    await callback.answer(text, show_alert=not ok)


@dp.callback_query(F.data == "subcheck:refresh")
async def subscription_refresh_callback(callback: CallbackQuery) -> None:
    subscribed, reason = await check_required_channel_subscription(callback.from_user.id)
    if subscribed:
        await callback.answer("Подписка подтверждена ✅", show_alert=True)
        if callback.message:
            await callback.message.answer(
                "✅ Подписка подтверждена. Теперь доступны все функции бота.",
                reply_markup=build_main_keyboard(callback.from_user.id),
            )
        return

    if reason == "member_list_inaccessible":
        await callback.answer(
            "Бот не видит подписчиков канала. Добавьте бота @boxvolt_bot админом в @BoxVoltVPN.",
            show_alert=True,
        )
        if callback.message:
            await send_subscription_required_prompt(
                callback.message,
                telegram_id=callback.from_user.id,
                check_reason=reason,
            )
        return

    await callback.answer("Подписка не найдена. Подпишитесь и нажмите снова.", show_alert=True)


@dp.callback_query(F.data == "guides:open")
async def guides_open_callback(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "📚 Выберите вашу ОС/приложение:\n"
        "Основные клиенты: Happ и V2rayTun.",
        reply_markup=build_guides_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("guide:"))
async def guide_item_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return

    key = callback.data.split(":", maxsplit=1)[1]
    guide = GUIDES.get(key)
    if not guide:
        await callback.answer("Инструкция не найдена", show_alert=True)
        return

    title, text = guide
    await callback.message.answer(f"{title}\n\n{text}")
    await callback.answer()


@dp.callback_query(F.data == "onboarding:done")
async def onboarding_done_callback(callback: CallbackQuery) -> None:
    user = get_user(callback.from_user.id)
    if not user or not has_active_subscription(user["subscription_end"]):
        await callback.answer("Активная подписка не найдена", show_alert=True)
        return
    await callback.answer("Отлично! Приятного использования 🚀", show_alert=False)
    if callback.message:
        await callback.message.answer(
            "✅ Подключение подтверждено.\n"
            "Если скорость/доступ нестабильны, используйте «👤 Личный кабинет» → «🛰 Переключить на резерв».",
            reply_markup=build_main_keyboard(callback.from_user.id),
        )


@dp.message(F.text == "🛟 Поддержка")
async def support_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)

    if not ADMIN_TELEGRAM_IDS:
        await message.answer(
            "🛟 Поддержка\n"
            f"Сейчас тикеты недоступны. Напишите напрямую: {SUPPORT_CONTACT}"
        )
        return

    open_ticket = get_latest_open_support_ticket_for_user(message.from_user.id)
    if open_ticket:
        ticket_id = int(open_ticket["id"])
        is_paused = message.from_user.id in USER_TICKET_CHAT_DISABLED
        USER_ACTIVE_TICKET_CHAT_BY_USER[message.from_user.id] = ticket_id
        USER_TICKET_CHAT_DISABLED.discard(message.from_user.id)
        if is_paused:
            await message.answer(
                f"🔓 Вы снова вошли в чат тикета #{ticket_id}.",
                reply_markup=build_support_ticket_keyboard(),
            )
            await send_ticket_dialog_history(
                message.from_user.id,
                ticket_id,
                header=f"🧾 Диалог тикета #{ticket_id}",
            )
            return
        await message.answer(
            f"🛟 Тикет #{ticket_id} уже открыт.\nПишите сюда как в обычный чат.",
            reply_markup=build_support_ticket_keyboard(),
        )
        return

    USER_TICKET_CHAT_DISABLED.discard(message.from_user.id)
    SUPPORT_WAITING_USERS.add(message.from_user.id)
    await message.answer(
        "🛟 Поддержка\n"
        "Опишите проблему текстом или отправьте изображение (можно с подписью).\n"
        "Администраторы увидят тикет и ответят здесь в боте.\n\n"
        "Для отмены отправьте: /cancel",
        reply_markup=build_support_ticket_keyboard(),
    )


@dp.message(Command("support"))
async def support_command_handler(message: Message) -> None:
    await support_handler(message)


@dp.message(F.text == SUPPORT_APPEND_BUTTON)
async def support_append_handler(message: Message) -> None:
    await support_handler(message)


@dp.message(F.text == SUPPORT_EXIT_BUTTON)
async def support_exit_handler(message: Message) -> None:
    if message.from_user.id in SUPPORT_WAITING_USERS:
        SUPPORT_WAITING_USERS.discard(message.from_user.id)
        await message.answer(
            "Создание тикета отменено.",
            reply_markup=build_main_keyboard(message.from_user.id),
        )
        return

    active_ticket_id = USER_ACTIVE_TICKET_CHAT_BY_USER.pop(message.from_user.id, None)
    if active_ticket_id:
        USER_TICKET_CHAT_DISABLED.add(message.from_user.id)
        await message.answer(
            f"🚪 Вы вышли из чата тикета #{active_ticket_id}.\n"
            "Тикет остается открытым. Чтобы вернуться в чат, откройте «🛟 Поддержка».",
            reply_markup=build_main_keyboard(message.from_user.id),
        )
        return

    open_ticket = get_latest_open_support_ticket_for_user(message.from_user.id)
    if open_ticket:
        USER_TICKET_CHAT_DISABLED.add(message.from_user.id)
        await message.answer(
            f"🚪 Режим чата для тикета #{open_ticket['id']} выключен.\n"
            "Чтобы вернуться, откройте «🛟 Поддержка».",
            reply_markup=build_main_keyboard(message.from_user.id),
        )
        return

    await message.answer(
        "Вы не находитесь в чате тикета.",
        reply_markup=build_main_keyboard(message.from_user.id),
    )


@dp.callback_query(F.data.startswith("tkt_take:"))
async def ticket_take_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Только для админов", show_alert=True)
        return
    upsert_user(callback.from_user.id, callback.from_user.username)

    raw_id = callback.data.split(":", maxsplit=1)[1]
    if not raw_id.isdigit():
        await callback.answer("Некорректный тикет", show_alert=True)
        return
    ticket_id = int(raw_id)

    ok, reason = take_support_ticket(ticket_id, callback.from_user.id)
    if not ok:
        if reason == "ticket_not_found":
            await callback.answer("Тикет не найден", show_alert=True)
            return
        if reason == "ticket_closed":
            await callback.answer("Тикет уже закрыт", show_alert=False)
            return
        if reason == "already_taken_by_other":
            ticket = get_support_ticket(ticket_id)
            assigned_label = "-"
            if ticket and ticket["assigned_admin_id"]:
                assigned_label = admin_label(int(ticket["assigned_admin_id"]))
            await callback.answer(f"Тикет уже взял администратор {assigned_label}", show_alert=False)
            return
        await callback.answer("Не удалось взять тикет", show_alert=True)
        return

    if reason == "taken":
        taker_label = admin_label(callback.from_user.id, callback.from_user.username)
        await notify_user_ticket_taken(ticket_id, callback.from_user.id)
        await refresh_ticket_for_admins(ticket_id, assigned_label=taker_label)
        await callback.answer("Тикет назначен вам", show_alert=False)
        return

    await refresh_ticket_for_admins(ticket_id, assigned_label=admin_label(callback.from_user.id))
    await callback.answer("Тикет уже у вас", show_alert=False)


@dp.callback_query(F.data.startswith("tkt_reply:"))
async def ticket_reply_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Только для админов", show_alert=True)
        return
    upsert_user(callback.from_user.id, callback.from_user.username)

    raw_id = callback.data.split(":", maxsplit=1)[1]
    if not raw_id.isdigit():
        await callback.answer("Некорректный тикет", show_alert=True)
        return
    ticket_id = int(raw_id)

    ticket, was_taken_now = ensure_support_ticket_in_progress(ticket_id, callback.from_user.id)
    if not ticket:
        await callback.answer("Тикет не найден", show_alert=True)
        return
    if str(ticket["status"]) == "closed":
        await callback.answer("Тикет уже закрыт", show_alert=False)
        return

    assigned_admin_id = ticket["assigned_admin_id"]
    if assigned_admin_id and int(assigned_admin_id) != callback.from_user.id:
        await callback.answer(
            f"Тикет уже ведет {admin_label(int(assigned_admin_id))}",
            show_alert=False,
        )
        return

    if was_taken_now:
        taker_label = admin_label(callback.from_user.id, callback.from_user.username)
        await notify_user_ticket_taken(ticket_id, callback.from_user.id)
        await refresh_ticket_for_admins(ticket_id, assigned_label=taker_label)

    ADMIN_REPLY_TICKET_BY_ADMIN[callback.from_user.id] = ticket_id
    await callback.answer("Режим чата включен", show_alert=False)
    await callback.message.answer(
        f"💬 Вы вошли в чат тикета #{ticket_id}.\n"
        "Все ваши следующие сообщения и изображения будут уходить пользователю.\n"
        "Выйти из чата: /cancel"
    )
    await send_ticket_dialog_history(
        callback.from_user.id,
        ticket_id,
        header=f"🧾 Диалог тикета #{ticket_id}",
    )


@dp.callback_query(F.data.startswith("tkt_hist:"))
async def ticket_history_callback(callback: CallbackQuery) -> None:
    await callback.answer("История отключена", show_alert=False)


@dp.callback_query(F.data.startswith("tkt_close:"))
async def ticket_close_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Только для админов", show_alert=True)
        return
    upsert_user(callback.from_user.id, callback.from_user.username)

    raw_id = callback.data.split(":", maxsplit=1)[1]
    if not raw_id.isdigit():
        await callback.answer("Некорректный тикет", show_alert=True)
        return
    ticket_id = int(raw_id)

    ok, reason = close_support_ticket(ticket_id, callback.from_user.id, closed_by="admin")
    if not ok:
        if reason == "ticket_not_found":
            await callback.answer("Тикет не найден", show_alert=True)
            return
        if reason == "already_closed":
            await callback.answer("Тикет уже закрыт", show_alert=False)
            return
        await callback.answer("Не удалось закрыть тикет", show_alert=True)
        return

    ticket = get_support_ticket(ticket_id)
    if ticket:
        USER_TICKET_CHAT_DISABLED.discard(int(ticket["telegram_id"]))
        try:
            await bot.send_message(
                int(ticket["telegram_id"]),
                f"✅ Тикет #{ticket_id} закрыт администратором.\n"
                "Если вопрос остался, создайте новый тикет через «🛟 Поддержка».",
                reply_markup=build_main_keyboard(int(ticket["telegram_id"])),
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[ticket] Failed to notify user about closed ticket {ticket_id}: {exc}")

    await refresh_ticket_for_admins(ticket_id)
    clear_ticket_chat_modes(ticket_id)

    await callback.answer("Тикет закрыт", show_alert=False)


@dp.message(F.text == "🛠 Админ")
async def admin_panel_text_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await show_admin_panel(message)


async def refresh_admin_panel_callback_message(callback: CallbackQuery) -> None:
    if not callback.message:
        return
    try:
        await callback.message.edit_text(
            format_admin_panel_text(),
            reply_markup=build_admin_panel_keyboard(),
        )
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            print(f"[adminctl] Failed to refresh admin panel: {exc}")


@dp.callback_query(F.data.startswith("adminctl:"))
async def admin_control_callback(callback: CallbackQuery) -> None:
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Только для админов", show_alert=True)
        return

    action = str(callback.data or "").split(":", maxsplit=1)[1].strip()
    if action == "toggle_update_mode":
        new_manual = not update_notify_manual_mode_enabled()
        set_update_notify_manual_mode(new_manual)
        await refresh_admin_panel_callback_message(callback)
        await callback.answer(
            "Режим обновлений: ручной" if new_manual else "Режим обновлений: авто",
            show_alert=False,
        )
        return

    if action == "send_update":
        sent, failed = await send_update_notice_broadcast()
        await refresh_admin_panel_callback_message(callback)
        if callback.message:
            await callback.message.answer(
                f"📣 Update-уведомление отправлено.\nОтправлено: {sent}\nОшибок: {failed}"
            )
        await callback.answer("Рассылка запущена", show_alert=False)
        return

    if action == "toggle_maintenance":
        enabled = not maintenance_mode_enabled()
        set_maintenance_mode(enabled)
        text = maintenance_broadcast_text(enabled)
        sent, failed = await broadcast_text(text)
        await refresh_admin_panel_callback_message(callback)
        if callback.message:
            await callback.message.answer(
                (
                    "🚧 Технические работы включены.\n"
                    "Выдача и перевыпуск ключей в боте временно заблокированы.\n"
                )
                if enabled
                else (
                    "✅ Технические работы выключены.\n"
                    "Выдача и перевыпуск ключей снова доступны.\n"
                )
                + f"Уведомлено: {sent}, ошибок: {failed}"
            )
        await callback.answer(
            "Техработы включены" if enabled else "Техработы выключены",
            show_alert=False,
        )
        return

    await refresh_admin_panel_callback_message(callback)
    await callback.answer("Панель обновлена", show_alert=False)


@dp.message(F.text == "🔥 Акции")
async def sale_handler(message: Message) -> None:
    sale_text = get_sale_text()
    blocks: list[str] = []
    if sale_text:
        blocks.append(sale_text)
    else:
        blocks.append("🔥 Акции BoxVolt\nСейчас активных скидок нет. Следите за обновлениями 👀")

    if TRIAL_ENABLED:
        blocks.append(f"🎁 Тест: {TRIAL_DAYS} {day_word(TRIAL_DAYS)} бесплатно.")

    sale_keyboard = build_sale_keyboard()
    reply_markup = sale_keyboard if sale_keyboard.inline_keyboard else None
    await message.answer("\n\n".join(blocks), reply_markup=reply_markup)


@dp.callback_query(F.data == "sale:open")
async def sale_open_callback(callback: CallbackQuery) -> None:
    if not callback.message:
        await callback.answer()
        return

    sale_text = get_sale_text()
    blocks: list[str] = []
    if sale_text:
        blocks.append(sale_text)
    else:
        blocks.append("🔥 Акции BoxVolt\nСейчас активных скидок нет. Следите за обновлениями 👀")
    if TRIAL_ENABLED:
        blocks.append(f"🎁 Тест: {TRIAL_DAYS} {day_word(TRIAL_DAYS)} бесплатно.")

    sale_keyboard = build_sale_keyboard()
    reply_markup = sale_keyboard if sale_keyboard.inline_keyboard else None
    await callback.message.answer("\n\n".join(blocks), reply_markup=reply_markup)
    await callback.answer()


async def handle_trial_request_for_user(
    telegram_id: int,
    username: str | None,
    answer: Callable[..., Awaitable[Any]],
) -> None:
    if not is_admin_user(telegram_id) and is_user_blacklisted(telegram_id):
        await answer("⛔ Доступ к боту ограничен. Обратитесь в поддержку.")
        return

    retry_after = trial_request_cooldown_left(telegram_id)
    if retry_after > 0:
        log_suspicious_flag(
            telegram_id,
            "trial_rate_limited",
            f"retry_after={retry_after}",
        )
        await answer(f"⏳ Слишком часто. Попробуйте снова через {retry_after} сек.")
        return
    mark_trial_request_seen(telegram_id)

    if not TRIAL_ENABLED:
        await answer("Тестовый период отключен.")
        return

    upsert_user(telegram_id, username)
    user = get_user(telegram_id)
    if not user:
        await answer("Пользователь не найден. Нажмите /start")
        return

    if int(user["trial_used"] or 0) == 1:
        log_suspicious_flag(telegram_id, "trial_reuse_attempt", "trial_used=1")
        await answer("❌ Тест уже использован.")
        return

    if has_trial_claim(telegram_id):
        log_suspicious_flag(telegram_id, "trial_reuse_attempt", "existing_trial_claim")
        await answer("❌ Тест уже использован.")
        return
    if TRIAL_USERNAME_UNIQUE and has_trial_claim_by_username(username):
        log_suspicious_flag(
            telegram_id,
            "trial_reuse_attempt",
            f"username_reused={str(username or '').strip().lower()}",
        )
        await answer("❌ Тест уже использован для этого username.")
        return

    if user_has_paid_payment(telegram_id):
        log_suspicious_flag(telegram_id, "trial_reuse_attempt", "already_has_paid_order")
        await answer("❌ Тест доступен только до первой оплаты.")
        return

    if parse_date(user["subscription_end"]):
        log_suspicious_flag(telegram_id, "trial_reuse_attempt", "subscription_end_already_set")
        await answer("❌ Тест уже был активирован ранее.")
        return

    conn = get_conn()
    inserted_trial_claim = conn.execute(
        """
        INSERT OR IGNORE INTO trial_claims (telegram_id, username, claimed_at)
        VALUES (?, ?, ?)
        """,
        (telegram_id, username, now_str()),
    )
    if int(inserted_trial_claim.rowcount or 0) == 0:
        conn.close()
        log_suspicious_flag(telegram_id, "trial_reuse_attempt", "insert_or_ignore_conflict")
        await answer("❌ Тест уже использован.")
        return

    end_at = dt.datetime.now() + dt.timedelta(days=TRIAL_DAYS)
    conn.execute(
        """
        UPDATE users
        SET trial_used = 1,
            subscription_end = ?
        WHERE telegram_id = ?
        """,
        (end_at.strftime("%Y-%m-%d %H:%M:%S"), telegram_id),
    )
    conn.commit()
    conn.close()

    fresh_user = get_user(telegram_id)
    if not fresh_user:
        await answer("✅ Тест активирован.")
        return

    if maintenance_mode_enabled():
        await answer(
            f"✅ Тест активирован на {TRIAL_DAYS} {day_word(TRIAL_DAYS)}.\n"
            f"До: {fresh_user['subscription_end']}\n\n"
            f"{maintenance_user_block_text()}\n"
            "После завершения работ нажмите «🚀 Подключить VPN»."
        )
        return

    try:
        await ensure_vless_uuid(
            telegram_id,
            fresh_user["vless_uuid"],
            fresh_user["subscription_end"],
        )
        subscription_block = build_subscription_text_block(telegram_id)
        if not subscription_block:
            raise RuntimeError("subscription_url_not_configured")
        await answer(
            f"✅ Тест активирован на {TRIAL_DAYS} {day_word(TRIAL_DAYS)}.\n"
            f"До: {fresh_user['subscription_end']}\n\n"
            "🔑 Ваш доступ:\n"
            f"{subscription_block}",
            parse_mode="HTML",
        )
    except Exception as exc:  # noqa: BLE001
        await answer(
            f"✅ Тест активирован на {TRIAL_DAYS} {day_word(TRIAL_DAYS)}.\n"
            "⚠️ URL-подписку не удалось выдать автоматически.\n"
            "Нажмите «🚀 Подключить VPN» или обратитесь в поддержку.\n"
            f"Техническая ошибка: {exc}"
        )


@dp.callback_query(F.data == "sale:trial")
async def sale_trial_callback(callback: CallbackQuery) -> None:
    if not callback.message:
        await callback.answer()
        return
    await handle_trial_request_for_user(
        telegram_id=callback.from_user.id,
        username=callback.from_user.username,
        answer=callback.message.answer,
    )
    await callback.answer()


async def send_referral_program_for_user(
    answer: Callable[..., Awaitable[Any]],
    telegram_id: int,
    username: str | None,
) -> None:
    if not REFERRAL_ENABLED:
        await answer("Реферальная программа сейчас отключена.")
        return
    upsert_user(telegram_id, username)
    text, keyboard = await build_referral_program_message(telegram_id)
    await answer(text, reply_markup=keyboard)


@dp.message(F.text == "👥 Реферальная программа")
async def referral_text_handler(message: Message) -> None:
    await send_referral_program_for_user(
        answer=message.answer,
        telegram_id=message.from_user.id,
        username=message.from_user.username,
    )


@dp.callback_query(F.data == "sale:referral")
async def sale_referral_callback(callback: CallbackQuery) -> None:
    if not callback.message:
        await callback.answer()
        return
    await send_referral_program_for_user(
        answer=callback.message.answer,
        telegram_id=callback.from_user.id,
        username=callback.from_user.username,
    )
    await callback.answer("Реферальная ссылка готова")


@dp.message(Command("trial"))
async def trial_command_handler(message: Message) -> None:
    await handle_trial_request_for_user(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        answer=message.answer,
    )


@dp.message(F.text.startswith("🎁 Тест на "))
async def trial_handler(message: Message) -> None:
    await handle_trial_request_for_user(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        answer=message.answer,
    )


@dp.message(Command("cancel"))
async def cancel_context_handler(message: Message) -> None:
    cancelled = False
    support_cancelled = False
    left_user_chat_ticket: int | None = None
    left_admin_chat_ticket: int | None = None

    if message.from_user.id in PROMO_WAITING_USERS:
        PROMO_WAITING_USERS.discard(message.from_user.id)
        cancelled = True

    if message.from_user.id in SUPPORT_WAITING_USERS:
        SUPPORT_WAITING_USERS.discard(message.from_user.id)
        cancelled = True
        support_cancelled = True

    if message.from_user.id in ADMIN_REPLY_TICKET_BY_ADMIN:
        left_admin_chat_ticket = ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
        cancelled = True

    if message.from_user.id in USER_ACTIVE_TICKET_CHAT_BY_USER:
        left_user_chat_ticket = USER_ACTIVE_TICKET_CHAT_BY_USER.pop(message.from_user.id, None)
        USER_TICKET_CHAT_DISABLED.add(message.from_user.id)
        cancelled = True

    if cancelled:
        if left_admin_chat_ticket:
            await message.answer(f"🚪 Вы вышли из чата тикета #{left_admin_chat_ticket}.")
            return
        if left_user_chat_ticket:
            await message.answer(
                f"🚪 Вы вышли из чата тикета #{left_user_chat_ticket}.",
                reply_markup=build_main_keyboard(message.from_user.id),
            )
            return
        if support_cancelled:
            await message.answer(
                "Отменено.",
                reply_markup=build_main_keyboard(message.from_user.id),
            )
            return
        await message.answer("Отменено.")
        return

    await message.answer("Нет активного действия для отмены.")


@dp.message(F.text)
async def text_context_handler(message: Message) -> None:
    text = (message.text or "").strip()
    if not text:
        return

    active_user_ticket_id = USER_ACTIVE_TICKET_CHAT_BY_USER.get(message.from_user.id)
    if active_user_ticket_id:
        active_ticket = get_support_ticket(int(active_user_ticket_id))
        if not active_ticket or str(active_ticket["status"]) == "closed":
            USER_ACTIVE_TICKET_CHAT_BY_USER.pop(message.from_user.id, None)

    # Режим ответа администратора на выбранный тикет.
    if is_admin_user(message.from_user.id) and message.from_user.id in ADMIN_REPLY_TICKET_BY_ADMIN:
        if text.lower() in {"отмена", "/cancel"}:
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
            await message.answer("🚪 Вы вышли из чата тикета.")
            return

        ticket_id = int(ADMIN_REPLY_TICKET_BY_ADMIN[message.from_user.id])
        ticket, was_taken_now = ensure_support_ticket_in_progress(ticket_id, message.from_user.id)
        if not ticket:
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
            await message.answer("Тикет не найден.")
            return
        if str(ticket["status"]) == "closed":
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
            await message.answer("Тикет уже закрыт.")
            return
        assigned_admin_id = ticket["assigned_admin_id"]
        if assigned_admin_id and int(assigned_admin_id) != message.from_user.id:
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
            await message.answer(f"Тикет уже ведет {admin_label(int(assigned_admin_id))}.")
            return
        if was_taken_now:
            taker_label = admin_label(message.from_user.id, message.from_user.username)
            await notify_user_ticket_taken(ticket_id, message.from_user.id)
            await refresh_ticket_for_admins(ticket_id, assigned_label=taker_label)

        try:
            await deliver_admin_message_to_user(
                ticket_id=ticket_id,
                message_text=text,
            )
            add_support_ticket_message(
                ticket_id=ticket_id,
                sender_role="admin",
                sender_id=message.from_user.id,
                message_text=text,
            )
            await refresh_ticket_for_admins(
                ticket_id,
                assigned_label=admin_label(message.from_user.id, message.from_user.username),
                only_assigned=True,
            )
        except Exception as exc:  # noqa: BLE001
            await message.answer(f"Не удалось отправить ответ: {exc}")
        return

    # Режим ввода промокода из профиля.
    if message.from_user.id in PROMO_WAITING_USERS:
        if text.lower() in {"отмена", "/cancel"}:
            PROMO_WAITING_USERS.discard(message.from_user.id)
            await message.answer("Ввод промокода отменен.")
            return

        activated, _ = await activate_promocode_for_message(message, text)
        if activated:
            PROMO_WAITING_USERS.discard(message.from_user.id)
        else:
            await message.answer("Попробуйте другой код или отправьте /cancel.")
        return

    # Режим создания тикета пользователем из меню поддержки.
    if message.from_user.id in SUPPORT_WAITING_USERS:
        if text.lower() in {"отмена", "/cancel"}:
            SUPPORT_WAITING_USERS.discard(message.from_user.id)
            await message.answer("Создание тикета отменено.")
            return

        if len(text) < 5:
            await message.answer("Опишите проблему подробнее (минимум 5 символов) или /cancel.")
            return

        SUPPORT_WAITING_USERS.discard(message.from_user.id)
        ticket_id = create_support_ticket(
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            message_text=text[:4000],
        )
        USER_TICKET_CHAT_DISABLED.discard(message.from_user.id)
        USER_ACTIVE_TICKET_CHAT_BY_USER[message.from_user.id] = ticket_id
        await notify_admins_new_ticket(ticket_id)
        await message.answer(
            f"✅ Тикет #{ticket_id} создан.\n"
            "Ожидайте: администратор возьмет тикет и ответит в этом чате.",
            reply_markup=build_support_ticket_keyboard(),
        )
        return

    # Пользователь может дописывать сообщения в уже открытый тикет.
    open_ticket = get_latest_open_support_ticket_for_user(message.from_user.id)
    if open_ticket and not text.startswith("/"):
        ticket_id = int(open_ticket["id"])
        if message.from_user.id in USER_TICKET_CHAT_DISABLED:
            await message.answer(
                f"Режим чата для тикета #{ticket_id} выключен.\n"
                "Откройте «🛟 Поддержка», чтобы снова войти в чат.",
                reply_markup=build_support_ticket_keyboard(),
            )
            return
        USER_TICKET_CHAT_DISABLED.discard(message.from_user.id)
        USER_ACTIVE_TICKET_CHAT_BY_USER[message.from_user.id] = ticket_id
        add_support_ticket_message(
            ticket_id=ticket_id,
            sender_role="user",
            sender_id=message.from_user.id,
            message_text=text[:4000],
        )
        await notify_admins_user_ticket_message(
            ticket_id=ticket_id,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            message_text=text[:4000],
        )
        await forward_user_ticket_message_to_admin_if_joined(
            ticket_id=ticket_id,
            message_text=text[:4000],
        )
        return


async def handle_support_media_message(
    message: Message,
    *,
    media_kind: str,
    media_file_id: str,
) -> bool:
    caption = str(message.caption or "").strip()
    message_text = caption[:4000] if caption else "Изображение без подписи."

    if message.from_user.id in SUPPORT_WAITING_USERS:
        SUPPORT_WAITING_USERS.discard(message.from_user.id)
        ticket_id = create_support_ticket(
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            message_text=message_text,
            media_kind=media_kind,
            media_file_id=media_file_id,
        )
        USER_TICKET_CHAT_DISABLED.discard(message.from_user.id)
        USER_ACTIVE_TICKET_CHAT_BY_USER[message.from_user.id] = ticket_id
        await notify_admins_new_ticket(ticket_id)
        await message.answer(
            f"✅ Тикет #{ticket_id} создан.\n"
            "Ожидайте: администратор возьмет тикет и ответит в этом чате.",
            reply_markup=build_support_ticket_keyboard(),
        )
        return True

    open_ticket = get_latest_open_support_ticket_for_user(message.from_user.id)
    if not open_ticket:
        return False

    ticket_id = int(open_ticket["id"])
    if message.from_user.id in USER_TICKET_CHAT_DISABLED:
        await message.answer(
            f"Режим чата для тикета #{ticket_id} выключен.\n"
            "Откройте «🛟 Поддержка», чтобы снова войти в чат.",
            reply_markup=build_support_ticket_keyboard(),
        )
        return True
    USER_TICKET_CHAT_DISABLED.discard(message.from_user.id)
    USER_ACTIVE_TICKET_CHAT_BY_USER[message.from_user.id] = ticket_id
    add_support_ticket_message(
        ticket_id=ticket_id,
        sender_role="user",
        sender_id=message.from_user.id,
        message_text=message_text,
        media_kind=media_kind,
        media_file_id=media_file_id,
    )
    await notify_admins_user_ticket_message(
        ticket_id=ticket_id,
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        message_text=message_text,
        media_kind=media_kind,
        media_file_id=media_file_id,
    )
    await forward_user_ticket_message_to_admin_if_joined(
        ticket_id=ticket_id,
        message_text=message_text,
        media_kind=media_kind,
        media_file_id=media_file_id,
        media_group_id=str(message.media_group_id) if message.media_group_id else None,
    )
    return True


async def handle_admin_ticket_media_message(
    message: Message,
    *,
    media_kind: str,
    media_file_id: str,
) -> bool:
    if not is_admin_user(message.from_user.id):
        return False
    ticket_id = ADMIN_REPLY_TICKET_BY_ADMIN.get(message.from_user.id)
    if not ticket_id:
        return False
    ticket_id = int(ticket_id)

    ticket, was_taken_now = ensure_support_ticket_in_progress(ticket_id, message.from_user.id)
    if not ticket:
        ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
        await message.answer("Тикет не найден. Режим чата отключен.")
        return True
    if str(ticket["status"]) == "closed":
        ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
        await message.answer("Тикет уже закрыт. Режим чата отключен.")
        return True

    assigned_admin_id = ticket["assigned_admin_id"]
    if assigned_admin_id and int(assigned_admin_id) != message.from_user.id:
        ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
        await message.answer(f"Тикет уже ведет {admin_label(int(assigned_admin_id))}.")
        return True

    if was_taken_now:
        taker_label = admin_label(message.from_user.id, message.from_user.username)
        await notify_user_ticket_taken(ticket_id, message.from_user.id)
        await refresh_ticket_for_admins(ticket_id, assigned_label=taker_label)

    caption_text = str(message.caption or "").strip()
    message_text = caption_text[:4000] if caption_text else "Изображение от администратора."
    try:
        await deliver_admin_message_to_user(
            ticket_id=ticket_id,
            message_text=message_text,
            media_kind=media_kind,
            media_file_id=media_file_id,
        )
        add_support_ticket_message(
            ticket_id=ticket_id,
            sender_role="admin",
            sender_id=message.from_user.id,
            message_text=message_text,
            media_kind=media_kind,
            media_file_id=media_file_id,
        )
        await refresh_ticket_for_admins(
            ticket_id,
            assigned_label=admin_label(message.from_user.id, message.from_user.username),
            only_assigned=True,
        )
    except Exception as exc:  # noqa: BLE001
        await message.answer(f"Не удалось отправить медиа пользователю: {exc}")
    return True


@dp.message(F.photo)
async def support_photo_context_handler(message: Message) -> None:
    if not message.photo:
        return
    if await handle_admin_ticket_media_message(
        message,
        media_kind="photo",
        media_file_id=str(message.photo[-1].file_id),
    ):
        return
    await handle_support_media_message(
        message,
        media_kind="photo",
        media_file_id=str(message.photo[-1].file_id),
    )


@dp.message(F.document)
async def support_image_document_context_handler(message: Message) -> None:
    document = message.document
    if not document:
        return
    mime_type = str(document.mime_type or "").lower()
    if not mime_type.startswith("image/"):
        return
    if await handle_admin_ticket_media_message(
        message,
        media_kind="document",
        media_file_id=str(document.file_id),
    ):
        return
    await handle_support_media_message(
        message,
        media_kind="document",
        media_file_id=str(document.file_id),
    )


async def start_webhook_server() -> web.AppRunner:
    app = make_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_HOST, WEBHOOK_PORT)
    await site.start()

    donatepay_webhook_path = _normalize_http_path(DONATEPAY_WEBHOOK_PATH, "/donatepay/webhook")
    cryptobot_webhook_path = _normalize_http_path(CRYPTOBOT_WEBHOOK_PATH, "/cryptobot/webhook")
    lzt_webhook_path = _normalize_http_path(LZT_WEBHOOK_PATH, "/lzt/webhook")
    subscription_path = _normalize_http_path(SUBSCRIPTION_PATH, "/sub").rstrip("/")
    if not subscription_path:
        subscription_path = "/sub"

    print(f"[webhook] DonatePay path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{donatepay_webhook_path}")
    print(f"[webhook] CryptoBot path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{cryptobot_webhook_path}")
    print(f"[webhook] LZT path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{lzt_webhook_path}")
    print(f"[webapp] Internal URL http://{WEBHOOK_HOST}:{WEBHOOK_PORT}/webapp")
    print(f"[pricing] File {pricing_path()}")
    if WEBAPP_PUBLIC_URL:
        print(f"[webapp] Public URL {WEBAPP_PUBLIC_URL}")
    public_base = resolved_public_base_url()
    if public_base:
        print(f"[sub] Public pattern {public_base}{subscription_path}/<sub_token>")
        print(f"[sub] Legacy pattern {public_base}{subscription_path}/<telegram_id>/<token>")
    return runner


async def main() -> None:
    ensure_pricing_file_exists()
    init_db()
    try:
        username = await get_bot_public_username()
        if username:
            print(f"[bot] Public username @{username}")
    except Exception as exc:  # noqa: BLE001
        print(f"[bot] Failed to resolve public username: {exc}")
    await maybe_send_update_notification()
    webhook_runner = await start_webhook_server()
    poll_tasks: list[asyncio.Task[None]] = []
    background_tasks: list[asyncio.Task[None]] = []
    if DONATEPAY_POLL_ENABLED and DONATEPAY_API_KEY and (
        get_active_payment_provider() == DONATEPAY_PROVIDER
        or has_pending_orders_for_provider(DONATEPAY_PROVIDER)
    ):
        poll_tasks.append(asyncio.create_task(donatepay_poll_loop(), name="dp-poll-loop"))
    if CRYPTOBOT_POLL_ENABLED and payment_provider_is_ready(CRYPTOBOT_PROVIDER):
        poll_tasks.append(asyncio.create_task(cryptobot_poll_loop(), name="cb-poll-loop"))
    if LZT_POLL_ENABLED and payment_provider_is_ready(LZT_PROVIDER):
        poll_tasks.append(asyncio.create_task(lzt_poll_loop(), name="lzt-poll-loop"))
    background_tasks.append(asyncio.create_task(payments_cleanup_loop(), name="payments-cleanup-loop"))
    if PENDING_ORDER_REMINDER_ENABLED:
        background_tasks.append(
            asyncio.create_task(
                pending_order_reminder_loop(),
                name="pending-order-reminder-loop",
            )
        )
    if SUBSCRIPTION_REMINDER_ENABLED:
        background_tasks.append(
            asyncio.create_task(
                subscription_expiry_reminder_loop(),
                name="sub-expiry-reminder-loop",
            )
        )
    if ADMIN_DAILY_REPORT_ENABLED:
        background_tasks.append(
            asyncio.create_task(
                admin_daily_report_loop(),
                name="admin-daily-report-loop",
            )
        )
    if SUPPORT_SLA_ENABLED:
        background_tasks.append(asyncio.create_task(support_sla_loop(), name="support-sla-loop"))
    if AUTO_BACKUP_ENABLED:
        background_tasks.append(asyncio.create_task(auto_backup_loop(), name="auto-backup-loop"))
    if SERVICE_MONITOR_ENABLED:
        background_tasks.append(asyncio.create_task(service_monitor_loop(), name="service-monitor-loop"))
    try:
        await dp.start_polling(bot)
    finally:
        for task in poll_tasks:
            task.cancel()
        for task in poll_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in background_tasks:
            task.cancel()
        for task in background_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await webhook_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
