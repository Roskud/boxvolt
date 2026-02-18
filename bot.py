import asyncio
import contextlib
import datetime as dt
import html
import hashlib
import hmac
import json
import os
import re
import sqlite3
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
REALITY_PROFILE_CACHE_SECONDS = int(env("REALITY_PROFILE_CACHE_SECONDS", "300"))

# DonationAlerts
DONATIONALERTS_USERNAME = env("DONATIONALERTS_USERNAME", "")
DONATIONALERTS_WEBHOOK_SECRET = env("DONATIONALERTS_WEBHOOK_SECRET", "")
DONATIONALERTS_WEBHOOK_PATH = env("DONATIONALERTS_WEBHOOK_PATH", "/donationalerts/webhook")
DONATIONALERTS_CLIENT_ID = env("DONATIONALERTS_CLIENT_ID", "")
DONATIONALERTS_CLIENT_SECRET = env("DONATIONALERTS_CLIENT_SECRET", "")
DONATIONALERTS_REDIRECT_URI = env("DONATIONALERTS_REDIRECT_URI", "")
DONATIONALERTS_OAUTH_SCOPES = env(
    "DONATIONALERTS_OAUTH_SCOPES",
    "oauth-user-show oauth-donation-index",
)
DONATIONALERTS_API_BASE = env("DONATIONALERTS_API_BASE", "https://www.donationalerts.com/api/v1")
DONATIONALERTS_OAUTH_AUTHORIZE_URL = env(
    "DONATIONALERTS_OAUTH_AUTHORIZE_URL",
    "https://www.donationalerts.com/oauth/authorize",
)
DONATIONALERTS_OAUTH_TOKEN_URL = env(
    "DONATIONALERTS_OAUTH_TOKEN_URL",
    "https://www.donationalerts.com/oauth/token",
)
DONATIONALERTS_POLL_ENABLED = env("DONATIONALERTS_POLL_ENABLED", "1") == "1"
DONATIONALERTS_POLL_INTERVAL_SECONDS = int(env("DONATIONALERTS_POLL_INTERVAL_SECONDS", "20"))
DONATIONALERTS_POLL_PAGE = int(env("DONATIONALERTS_POLL_PAGE", "1"))
DONATIONALERTS_OAUTH_CALLBACK_PATH = env(
    "DONATIONALERTS_OAUTH_CALLBACK_PATH",
    "/donationalerts/oauth/callback",
)
DONATIONALERTS_OAUTH_URL_PATH = env("DONATIONALERTS_OAUTH_URL_PATH", "/donationalerts/oauth/url")
DONATIONALERTS_SYNC_SECRET = env("DONATIONALERTS_SYNC_SECRET", "")
DONATIONALERTS_SYNC_PATH = env("DONATIONALERTS_SYNC_PATH", "/donationalerts/sync")
DONATIONALERTS_OAUTH_STATE = env("DONATIONALERTS_OAUTH_STATE", "")
DONATIONALERTS_TOKEN_FILE = env(
    "DONATIONALERTS_TOKEN_FILE",
    str(BASE_DIR / "donationalerts_token.json"),
)
DONATIONALERTS_DONATE_BASE_URL = env("DONATIONALERTS_DONATE_BASE_URL", "").strip()

# DonatePay
DONATEPAY_API_KEY = env("DONATEPAY_API_KEY", "").strip()
DONATEPAY_API_BASE = env("DONATEPAY_API_BASE", "https://donatepay.ru/api/v1").strip().rstrip("/")
DONATEPAY_DONATE_BASE_URL = env("DONATEPAY_DONATE_BASE_URL", "").strip()
DONATEPAY_WEBHOOK_SECRET = env("DONATEPAY_WEBHOOK_SECRET", "").strip()
DONATEPAY_WEBHOOK_PATH = env("DONATEPAY_WEBHOOK_PATH", "/donatepay/webhook").strip()
DONATEPAY_POLL_ENABLED = env("DONATEPAY_POLL_ENABLED", "1") == "1"
DONATEPAY_POLL_INTERVAL_SECONDS = int(env("DONATEPAY_POLL_INTERVAL_SECONDS", "20"))
DONATEPAY_POLL_LIMIT = max(1, int(env("DONATEPAY_POLL_LIMIT", "50")))

# Payment provider: donatepay | donationalerts.
PAYMENT_PROVIDER = env("PAYMENT_PROVIDER", "").strip().lower()

APP_VERSION = env("APP_VERSION", "").strip()
UPDATE_NOTIFY_ON_START = env("UPDATE_NOTIFY_ON_START", "1") == "1"
UPDATE_NOTIFY_TEXT = env(
    "UPDATE_NOTIFY_TEXT",
    "🆕 Вышло новое обновление. Пропишите /start",
).strip()

# Bot settings
SUPPORT_CONTACT = env("SUPPORT_CONTACT", "@boxvolt_support")
REQUIRE_CHANNEL_SUBSCRIPTION = env("REQUIRE_CHANNEL_SUBSCRIPTION", "1") == "1"
REQUIRED_CHANNEL_USERNAME = env("REQUIRED_CHANNEL_USERNAME", "@BoxVoltVPN").strip()
REQUIRED_CHANNEL_URL = env("REQUIRED_CHANNEL_URL", "").strip()
SUBSCRIPTION_PROMPT_COOLDOWN_SECONDS = max(
    1,
    int(env("SUBSCRIPTION_PROMPT_COOLDOWN_SECONDS", "15")),
)
TRIAL_ENABLED = env("TRIAL_ENABLED", "0") == "1"
TRIAL_DAYS = int(env("TRIAL_DAYS", "3"))
WEBAPP_PUBLIC_URL = env("WEBAPP_PUBLIC_URL", "")
WEBAPP_INITDATA_MAX_AGE_SECONDS = int(env("WEBAPP_INITDATA_MAX_AGE_SECONDS", "86400"))
PRICING_FILE = env("PRICING_FILE", str(BASE_DIR / "pricing.json"))
ADMIN_TELEGRAM_IDS_RAW = env("ADMIN_TELEGRAM_IDS", "")
PAYMENT_PENDING_TTL_MINUTES = int(env("PAYMENT_PENDING_TTL_MINUTES", "15"))
PAYMENT_CLEANUP_INTERVAL_SECONDS = int(env("PAYMENT_CLEANUP_INTERVAL_SECONDS", "60"))
PUBLIC_BASE_URL = env("PUBLIC_BASE_URL", "")
SUBSCRIPTION_PATH = env("SUBSCRIPTION_PATH", "/sub")
SUBSCRIPTION_SECRET = env(
    "SUBSCRIPTION_SECRET",
    DONATEPAY_WEBHOOK_SECRET or DONATIONALERTS_WEBHOOK_SECRET or BOT_TOKEN,
)
SUBSCRIPTION_PROFILE_TITLE = env("SUBSCRIPTION_PROFILE_TITLE", "BoxVolt Технология 3.0")
SUBSCRIPTION_UPDATE_INTERVAL_HOURS = max(1, int(env("SUBSCRIPTION_UPDATE_INTERVAL_HOURS", "1")))

# Web server for webhook
WEBHOOK_HOST = env("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT = int(env("WEBHOOK_PORT", "8080"))

DEFAULT_PLANS: dict[str, Plan] = {
    "d1": Plan(
        code="d1",
        title="1 день",
        days=1,
        amount_rub=5,
    ),
    "w1": Plan(
        code="w1",
        title="7 дней",
        days=7,
        amount_rub=35,
    ),
    "w2": Plan(
        code="w2",
        title="14 дней",
        days=14,
        amount_rub=65,
    ),
    "m1": Plan(
        code="m1",
        title="30 дней",
        days=30,
        amount_rub=100,
    ),
    "m2": Plan(
        code="m2",
        title="60 дней",
        days=60,
        amount_rub=150,
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
PROMO_CODE_RE = re.compile(r"^[A-Z0-9_-]{3,32}$")
ORDER_STATUS_MARKER = "\n\n📌 Статус заказа:\n"
PROCESS_LOCK = asyncio.Lock()
WEBAPP_TEMPLATE_PATH = BASE_DIR / "webapp" / "index.html"
DA_PROVIDER = "donationalerts_oauth"
DONATEPAY_PROVIDER = "donatepay"
DA_TOKEN_LOCK = asyncio.Lock()
DA_TOKEN_CACHE: dict[str, Any] | None = None
DA_LAST_DONATION_ID = 0
DA_OAUTH_HINT_PRINTED = False
DONATEPAY_LAST_TRANSACTION_ID = 0
PRICING_CACHE: dict[str, Any] | None = None
PRICING_CACHE_MTIME: float | None = None
REALITY_PROFILE_CACHE: dict[str, str] | None = None
REALITY_PROFILE_CACHE_AT: dt.datetime | None = None
ADMIN_TELEGRAM_IDS = {
    int(value.strip())
    for value in ADMIN_TELEGRAM_IDS_RAW.split(",")
    if value.strip().isdigit()
}
SUPPORT_WAITING_USERS: set[int] = set()
ADMIN_REPLY_TICKET_BY_ADMIN: dict[int, int] = {}
SUPPORT_TICKET_ADMIN_MESSAGE_IDS: dict[int, dict[int, int]] = {}
PROMO_WAITING_USERS: set[int] = set()
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
            subscription_end TEXT,
            vless_uuid TEXT,
            trial_used INTEGER DEFAULT 0
        )
        """
    )

    if not _column_exists(conn, "users", "vless_uuid"):
        cursor.execute("ALTER TABLE users ADD COLUMN vless_uuid TEXT")
    if not _column_exists(conn, "users", "trial_used"):
        cursor.execute("ALTER TABLE users ADD COLUMN trial_used INTEGER DEFAULT 0")

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
        CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT,
            initial_message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            assigned_admin_id INTEGER,
            created_at TEXT NOT NULL,
            taken_at TEXT,
            closed_at TEXT
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
            created_at TEXT NOT NULL,
            FOREIGN KEY (ticket_id) REFERENCES support_tickets (id)
        )
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
        if isinstance(event, CallbackQuery):
            callback_data = str(event.data or "")
            if callback_data.startswith("subcheck:"):
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


def upsert_user(telegram_id: int, username: str | None) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)",
        (telegram_id, username),
    )
    conn.execute(
        "UPDATE users SET username = COALESCE(?, username) WHERE telegram_id = ?",
        (username, telegram_id),
    )
    conn.commit()
    conn.close()


def get_user(telegram_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT telegram_id, username, subscription_end, vless_uuid, trial_used FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return row


def get_all_user_ids() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT telegram_id FROM users ORDER BY telegram_id").fetchall()
    conn.close()
    return [int(row["telegram_id"]) for row in rows]


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


async def maybe_send_update_notification() -> None:
    if not UPDATE_NOTIFY_ON_START:
        return
    if not APP_VERSION:
        return

    last_version = get_app_meta("update_notice_version")
    if last_version == APP_VERSION:
        return

    text = UPDATE_NOTIFY_TEXT or "🆕 Вышло новое обновление. Пропишите /start"
    sent, failed = await broadcast_text(text)
    set_app_meta("update_notice_version", APP_VERSION)
    print(
        f"[update-notify] version={APP_VERSION} sent={sent} failed={failed} text={text[:120]}"
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


def create_support_ticket(telegram_id: int, username: str | None, message_text: str) -> int:
    conn = get_conn()
    cursor = conn.execute(
        """
        INSERT INTO support_tickets (
            telegram_id, username, initial_message, status, created_at
        )
        VALUES (?, ?, ?, 'open', ?)
        """,
        (telegram_id, username, message_text, now_str()),
    )
    ticket_id = int(cursor.lastrowid)
    conn.execute(
        """
        INSERT INTO support_ticket_messages (ticket_id, sender_role, sender_id, message, created_at)
        VALUES (?, 'user', ?, ?, ?)
        """,
        (ticket_id, telegram_id, message_text, now_str()),
    )
    conn.commit()
    conn.close()
    return ticket_id


def add_support_ticket_message(
    ticket_id: int,
    sender_role: str,
    sender_id: int | None,
    message_text: str,
) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO support_ticket_messages (ticket_id, sender_role, sender_id, message, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (ticket_id, sender_role, sender_id, message_text, now_str()),
    )
    conn.commit()
    conn.close()


def get_support_ticket(ticket_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT id, telegram_id, username, initial_message, status, assigned_admin_id, created_at, taken_at, closed_at
        FROM support_tickets
        WHERE id = ?
        """,
        (ticket_id,),
    ).fetchone()
    conn.close()
    return row


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

    conn = get_conn()
    conn.execute(
        """
        UPDATE support_tickets
        SET assigned_admin_id = ?, status = 'in_progress', taken_at = ?
        WHERE id = ? AND assigned_admin_id IS NULL
        """,
        (admin_id, now_str(), ticket_id),
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

    conn = get_conn()
    conn.execute(
        """
        UPDATE support_tickets
        SET assigned_admin_id = ?, status = 'in_progress', taken_at = ?
        WHERE id = ? AND assigned_admin_id IS NULL
        """,
        (admin_id, now_str(), ticket_id),
    )
    conn.commit()
    conn.close()
    return get_support_ticket(ticket_id), True


def close_support_ticket(ticket_id: int, admin_id: int | None = None) -> tuple[bool, str]:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return False, "ticket_not_found"
    if str(ticket["status"]) == "closed":
        return False, "already_closed"

    conn = get_conn()
    conn.execute(
        """
        UPDATE support_tickets
        SET status = 'closed',
            closed_at = ?,
            assigned_admin_id = COALESCE(assigned_admin_id, ?)
        WHERE id = ?
        """,
        (now_str(), admin_id, ticket_id),
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

    if status == "closed":
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
        f"Статус: {status_line}\n\n"
        f"Сообщение:\n{ticket['initial_message']}"
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
    if raw in {"donatepay", DONATEPAY_PROVIDER}:
        return DONATEPAY_PROVIDER
    return "donationalerts"


def get_active_payment_provider() -> str:
    if PAYMENT_PROVIDER:
        return normalize_payment_provider(PAYMENT_PROVIDER)
    if DONATEPAY_API_KEY and DONATEPAY_DONATE_BASE_URL:
        return DONATEPAY_PROVIDER
    return "donationalerts"


def payment_provider_label(provider: str | None = None) -> str:
    active = normalize_payment_provider(provider or get_active_payment_provider())
    if active == DONATEPAY_PROVIDER:
        return "DonatePay"
    return "DonationAlerts"


def payment_provider_is_ready(provider: str | None = None) -> bool:
    active = normalize_payment_provider(provider or get_active_payment_provider())
    if active == DONATEPAY_PROVIDER:
        return bool(DONATEPAY_DONATE_BASE_URL and DONATEPAY_API_KEY)
    return bool(DONATIONALERTS_USERNAME or DONATIONALERTS_DONATE_BASE_URL)


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
        [KeyboardButton(text="🛟 Поддержка"), KeyboardButton(text="🔥 Акции")],
    ]
    if TRIAL_ENABLED:
        keyboard_rows.append([KeyboardButton(text=f"🎁 Тест на {TRIAL_DAYS} дня")])
    if telegram_id is not None and is_admin_user(telegram_id):
        keyboard_rows.append([KeyboardButton(text="🛠 Админ")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard_rows,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие",
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
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    mini_app_url = build_webapp_order_url(order_id, auto_pay=True)
    if mini_app_url:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🧩 Оплатить в Mini App",
                    web_app=WebAppInfo(url=mini_app_url),
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"💸 Оплатить в {payment_provider_label(provider)}",
                    url=payment_url,
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


def build_order_closed_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")],
        ]
    )


def build_profile_keyboard(subscription_active: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="profile:promo")],
        [InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")],
    ]
    if subscription_active:
        rows.insert(
            0,
            [InlineKeyboardButton(text="🔄 Перевыпустить ключ", callback_data="profile:reissue")],
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


def build_webapp_order_url(order_id: str, auto_pay: bool = False) -> str:
    base = (WEBAPP_PUBLIC_URL or "").strip()
    if not base:
        return ""
    separator = "&" if "?" in base else "?"
    order_arg = quote(str(order_id).strip(), safe="")
    auto_arg = "1" if auto_pay else "0"
    return f"{base}{separator}order_id={order_arg}&autopay={auto_arg}"


def _append_query_params(base: str, params: dict[str, str]) -> str:
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}{urlencode(params)}"


def build_donationalerts_url(order_id: str, amount_rub: int) -> str:
    base = (
        DONATIONALERTS_DONATE_BASE_URL
        or f"https://www.donationalerts.com/r/{DONATIONALERTS_USERNAME}"
    )
    if "dalink.to" in base.lower():
        base = f"https://www.donationalerts.com/r/{DONATIONALERTS_USERNAME}"
    clean_order_id = str(order_id).strip().upper()
    amount_text = str(int(amount_rub))
    amount_dot = f"{int(amount_rub)}.00"
    params = {
        "amount": amount_text,
        "default_amount": amount_text,
        "sum": amount_text,
        "donation_amount": amount_text,
        "value": amount_text,
        "amount_rub": amount_text,
        "amount_float": amount_dot,
        "currency": "RUB",
        "message": clean_order_id,
        "comment": clean_order_id,
        "text": clean_order_id,
        "donation_text": clean_order_id,
        "donation_message": clean_order_id,
        "default_comment": clean_order_id,
        "description": clean_order_id,
        "order_id": clean_order_id,
        "utm_source": "boxvolt_bot",
        "utm_medium": "telegram",
        "utm_campaign": clean_order_id,
    }
    return _append_query_params(base, params)


def build_donatepay_url(order_id: str, amount_rub: int) -> str:
    base = DONATEPAY_DONATE_BASE_URL
    if not base:
        return ""

    clean_order_id = str(order_id).strip().upper()
    amount_text = str(int(amount_rub))
    amount_dot = f"{int(amount_rub)}.00"
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
    return _append_query_params(base, params)


def build_payment_url(
    order_id: str,
    amount_rub: int,
    provider: str | None = None,
) -> str:
    active = normalize_payment_provider(provider or get_active_payment_provider())
    if active == DONATEPAY_PROVIDER:
        return build_donatepay_url(order_id, amount_rub)
    return build_donationalerts_url(order_id, amount_rub)


def build_donation_url(order_id: str, amount_rub: int) -> str:
    # Backward-compatible wrapper.
    return build_payment_url(order_id, amount_rub)


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

    for candidate in (WEBAPP_PUBLIC_URL, DONATIONALERTS_REDIRECT_URI):
        raw = (candidate or "").strip()
        if not raw:
            continue
        parsed = urlsplit(raw)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def build_subscription_token(telegram_id: int) -> str:
    secret = (SUBSCRIPTION_SECRET or BOT_TOKEN).encode("utf-8")
    payload = f"sub:{telegram_id}".encode("utf-8")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()[:32]


def is_valid_subscription_token(telegram_id: int, token: str) -> bool:
    provided = (token or "").strip().lower()
    expected = build_subscription_token(telegram_id)
    return bool(provided) and hmac.compare_digest(provided, expected)


def build_subscription_url(telegram_id: int) -> str:
    base = resolved_public_base_url()
    if not base:
        return ""
    path = _normalize_http_path(SUBSCRIPTION_PATH, "/sub").rstrip("/")
    token = build_subscription_token(telegram_id)
    return f"{base}{path}/{telegram_id}/{token}"


def build_subscription_profile_url(telegram_id: int) -> str:
    sub_url = build_subscription_url(telegram_id)
    if not sub_url:
        return ""
    return f"{sub_url}/profile"


def subscription_total_bytes() -> int:
    if XUI_TOTAL_GB <= 0:
        return 0
    return XUI_TOTAL_GB * 1024 * 1024 * 1024


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


def donationalerts_oauth_configured() -> bool:
    return bool(
        DONATIONALERTS_CLIENT_ID
        and DONATIONALERTS_CLIENT_SECRET
        and DONATIONALERTS_REDIRECT_URI
    )


def donationalerts_oauth_state() -> str:
    seed = DONATIONALERTS_OAUTH_STATE or DONATIONALERTS_SYNC_SECRET or DONATIONALERTS_WEBHOOK_SECRET
    if not seed:
        return ""
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]


def build_donationalerts_oauth_url() -> str:
    params = {
        "client_id": DONATIONALERTS_CLIENT_ID,
        "redirect_uri": DONATIONALERTS_REDIRECT_URI,
        "response_type": "code",
        "scope": DONATIONALERTS_OAUTH_SCOPES,
    }
    state = donationalerts_oauth_state()
    if state:
        params["state"] = state
    return f"{DONATIONALERTS_OAUTH_AUTHORIZE_URL}?{urlencode(params)}"


def _token_file_path() -> Path:
    return Path(DONATIONALERTS_TOKEN_FILE)


def load_donationalerts_token() -> dict[str, Any] | None:
    path = _token_file_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        return None
    payload["access_token"] = access_token
    return payload


def save_donationalerts_token(token: dict[str, Any]) -> None:
    path = _token_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(token)
    payload["updated_at"] = now_str()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_donationalerts_token(
    payload: dict[str, Any],
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prev = previous or {}
    now_ts = int(dt.datetime.now().timestamp())

    expires_at = int(prev.get("expires_at") or 0)
    expires_in_raw = payload.get("expires_in")
    try:
        expires_in = int(str(expires_in_raw))
    except (TypeError, ValueError):
        expires_in = 0
    if expires_in > 0:
        expires_at = now_ts + expires_in

    return {
        "access_token": str(payload.get("access_token") or prev.get("access_token") or "").strip(),
        "refresh_token": str(payload.get("refresh_token") or prev.get("refresh_token") or "").strip(),
        "token_type": str(payload.get("token_type") or prev.get("token_type") or "Bearer").strip(),
        "expires_at": expires_at,
        "scope": str(payload.get("scope") or prev.get("scope") or DONATIONALERTS_OAUTH_SCOPES),
    }


async def request_donationalerts_token(data: dict[str, str]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            DONATIONALERTS_OAUTH_TOKEN_URL,
            data=data,
            headers={"Accept": "application/json"},
        )

    if response.status_code >= 400:
        body = response.text.strip().replace("\n", " ")[:250]
        raise RuntimeError(f"oauth_token_http_{response.status_code}: {body}")

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"oauth_token_invalid_json: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("oauth_token_bad_payload")
    return payload


async def exchange_donationalerts_code(code: str) -> dict[str, Any]:
    if not donationalerts_oauth_configured():
        raise RuntimeError("oauth_not_configured")

    payload = await request_donationalerts_token(
        {
            "grant_type": "authorization_code",
            "client_id": DONATIONALERTS_CLIENT_ID,
            "client_secret": DONATIONALERTS_CLIENT_SECRET,
            "redirect_uri": DONATIONALERTS_REDIRECT_URI,
            "code": code,
        }
    )
    token = normalize_donationalerts_token(payload)
    if not token["access_token"]:
        raise RuntimeError("oauth_token_missing_access_token")
    return token


async def refresh_donationalerts_token(current: dict[str, Any]) -> dict[str, Any]:
    refresh_token = str(current.get("refresh_token") or "").strip()
    if not refresh_token:
        raise RuntimeError("oauth_refresh_token_missing")

    payload = await request_donationalerts_token(
        {
            "grant_type": "refresh_token",
            "client_id": DONATIONALERTS_CLIENT_ID,
            "client_secret": DONATIONALERTS_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "redirect_uri": DONATIONALERTS_REDIRECT_URI,
            "scope": DONATIONALERTS_OAUTH_SCOPES,
        }
    )
    token = normalize_donationalerts_token(payload, current)
    if not token["access_token"]:
        raise RuntimeError("oauth_refresh_missing_access_token")
    return token


async def set_donationalerts_token(token: dict[str, Any]) -> None:
    global DA_TOKEN_CACHE
    async with DA_TOKEN_LOCK:
        DA_TOKEN_CACHE = token
        save_donationalerts_token(token)


async def get_donationalerts_access_token(force_refresh: bool = False) -> str | None:
    global DA_TOKEN_CACHE

    async with DA_TOKEN_LOCK:
        token = DA_TOKEN_CACHE
        if token is None:
            token = load_donationalerts_token()
            DA_TOKEN_CACHE = token
        if not token:
            return None

        now_ts = int(dt.datetime.now().timestamp())
        expires_at = int(token.get("expires_at") or 0)
        token_expiring = bool(expires_at and expires_at <= now_ts + 60)
        should_refresh = force_refresh or token_expiring

        if should_refresh:
            if not donationalerts_oauth_configured():
                return None
            if not token.get("refresh_token"):
                if force_refresh:
                    return None
            else:
                refreshed = await refresh_donationalerts_token(token)
                DA_TOKEN_CACHE = refreshed
                token = refreshed
                save_donationalerts_token(token)

        access_token = str(token.get("access_token") or "").strip()
        return access_token or None


def donation_item_id(item: dict[str, Any]) -> int:
    raw = item.get("id")
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return 0


async def fetch_donationalerts_donations(access_token: str) -> list[dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    params = {"page": DONATIONALERTS_POLL_PAGE}

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            f"{DONATIONALERTS_API_BASE}/alerts/donations",
            headers=headers,
            params=params,
        )

    if response.status_code == 401:
        raise PermissionError("oauth_unauthorized")
    if response.status_code >= 400:
        body = response.text.strip().replace("\n", " ")[:250]
        raise RuntimeError(f"donations_http_{response.status_code}: {body}")

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"donations_invalid_json: {exc}") from exc

    data = payload.get("data")
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


async def process_donationalerts_sync(access_token: str) -> dict[str, int]:
    global DA_LAST_DONATION_ID

    donations = await fetch_donationalerts_donations(access_token)
    if not donations:
        return {"checked": 0, "matched": 0, "processed": 0}

    checked = 0
    matched = 0
    processed = 0
    max_seen = DA_LAST_DONATION_ID

    for donation in sorted(donations, key=donation_item_id):
        donation_id = donation_item_id(donation)
        if donation_id <= 0:
            continue
        if donation_id and donation_id <= DA_LAST_DONATION_ID:
            continue
        checked += 1
        if donation_id > max_seen:
            max_seen = donation_id

        payload = {
            "provider": DA_PROVIDER,
            "event": "donation",
            "status": "paid",
            "amount": donation.get("amount"),
            "message": donation.get("message"),
            "data": donation,
        }

        order_id, match_reason = resolve_order_id_from_payload(payload)
        if not order_id:
            continue
        matched += 1

        ok, reason = await process_paid_order(order_id, payload)
        if ok:
            processed += 1
            print(f"[da-poll] Order {order_id} processed ({reason}, by={match_reason})")
        else:
            print(f"[da-poll] Order {order_id} skipped ({reason}, by={match_reason})")

    if max_seen > DA_LAST_DONATION_ID:
        DA_LAST_DONATION_ID = max_seen

    return {"checked": checked, "matched": matched, "processed": processed}


async def donationalerts_poll_loop() -> None:
    global DA_OAUTH_HINT_PRINTED

    interval = max(5, DONATIONALERTS_POLL_INTERVAL_SECONDS)
    while True:
        try:
            if not donationalerts_oauth_configured():
                if not DA_OAUTH_HINT_PRINTED:
                    print(
                        "[da-poll] OAuth not configured. "
                        "Set DONATIONALERTS_CLIENT_ID, DONATIONALERTS_CLIENT_SECRET, DONATIONALERTS_REDIRECT_URI."
                    )
                    DA_OAUTH_HINT_PRINTED = True
                await asyncio.sleep(interval)
                continue

            access_token = await get_donationalerts_access_token()
            if not access_token:
                if not DA_OAUTH_HINT_PRINTED:
                    print("[da-poll] OAuth token missing. Open this URL once to connect DonationAlerts:")
                    print(f"[da-poll] {build_donationalerts_oauth_url()}")
                    DA_OAUTH_HINT_PRINTED = True
                await asyncio.sleep(interval)
                continue

            stats = await process_donationalerts_sync(access_token)
            DA_OAUTH_HINT_PRINTED = False
            if stats["processed"] > 0:
                print(f"[da-poll] Processed {stats['processed']} payment(s) this cycle")

        except PermissionError:
            try:
                refreshed = await get_donationalerts_access_token(force_refresh=True)
            except Exception as exc:  # noqa: BLE001
                refreshed = None
                print(f"[da-poll] Token refresh failed: {exc}")
            if not refreshed:
                print("[da-poll] 401 from API. Re-authorize DonationAlerts OAuth.")

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[da-poll] Sync error: {exc}")

        await asyncio.sleep(interval)


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


async def sync_pending_payment_order(order_id: str) -> tuple[bool, str]:
    payment = get_payment(order_id, apply_expiry=True)
    if not payment:
        return False, "order_not_found"
    if payment["status"] == "paid":
        return True, "already_paid"
    if payment["status"] != "pending":
        return False, f"not_pending:{payment['status']}"

    provider = normalize_payment_provider(payment["provider"])
    if provider == DONATEPAY_PROVIDER:
        if not DONATEPAY_API_KEY:
            return False, "donatepay_api_key_missing"
        await process_donatepay_sync(target_order_id=order_id)
    else:
        access_token = await get_donationalerts_access_token()
        if not access_token:
            return False, "oauth_token_missing"
        try:
            await process_donationalerts_sync(access_token)
        except PermissionError:
            refreshed = await get_donationalerts_access_token(force_refresh=True)
            if not refreshed:
                return False, "oauth_unauthorized"
            await process_donationalerts_sync(refreshed)

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
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[payments] Cleanup error: {exc}")

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


async def xui_get_inbound(client: httpx.AsyncClient, cookies: httpx.Cookies) -> dict[str, Any]:
    resp = await client.get(f"{XUI_URL}/panel/api/inbounds/get/{INBOUND_ID}", cookies=cookies)
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
) -> dict[str, Any]:
    payload = dict(existing_client or {})
    payload["id"] = client_uuid
    payload["email"] = str(telegram_id)
    if XUI_FLOW != "":
        payload["flow"] = XUI_FLOW
    else:
        payload["flow"] = str(payload.get("flow") or fallback_flow)
    payload["limitIp"] = max(0, XUI_LIMIT_IP)
    payload["totalGB"] = max(0, XUI_TOTAL_GB)
    payload["expiryTime"] = max(0, expiry_time_ms)
    payload["enable"] = True

    if not payload.get("subId"):
        payload["subId"] = uuid.uuid4().hex[:16]
    return payload


async def xui_upsert_client(
    telegram_id: int,
    preferred_uuid: str | None,
    subscription_end: str | None,
) -> str:
    if not all([XUI_URL, XUI_USERNAME, XUI_PASSWORD]):
        raise RuntimeError("3x-ui config is incomplete in .env")

    expiry_time_ms = subscription_end_to_ms(subscription_end)

    async with httpx.AsyncClient(timeout=20.0) as client:
        cookies = await xui_login(client)
        inbound_obj = await xui_get_inbound(client, cookies)
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
                if str(item.get("email") or "") == str(telegram_id):
                    found_client = item
                    break

        client_uuid = str(found_client.get("id") or "") if found_client else ""
        if not client_uuid:
            client_uuid = preferred_uuid or str(uuid.uuid4())

        payload = build_xui_client_payload(
            telegram_id=telegram_id,
            client_uuid=client_uuid,
            expiry_time_ms=expiry_time_ms,
            existing_client=found_client,
            fallback_flow=fallback_flow,
        )
        settings_payload = {"clients": [payload]}

        if found_client:
            resp = await client.post(
                f"{XUI_URL}/panel/api/inbounds/updateClient/{client_uuid}",
                data={"id": INBOUND_ID, "settings": json.dumps(settings_payload, ensure_ascii=False)},
                cookies=cookies,
            )
        else:
            resp = await client.post(
                f"{XUI_URL}/panel/api/inbounds/addClient",
                data={"id": INBOUND_ID, "settings": json.dumps(settings_payload, ensure_ascii=False)},
                cookies=cookies,
            )
        resp.raise_for_status()

        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"3x-ui upsert client invalid json: {exc}") from exc

        if not isinstance(body, dict) or body.get("success") is False:
            raise RuntimeError(f"3x-ui upsert client failed: {body}")

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
    profile = await get_reality_profile()
    params = {
        "encryption": "none",
        "type": "tcp",
        "security": "reality",
        "sni": profile.get("sni") or SNI,
        "fp": profile.get("fingerprint") or UTLS_FP,
        "pbk": profile.get("public_key") or PUBLIC_KEY,
        "sid": profile.get("short_id") or SHORT_ID,
    }
    if XUI_FLOW:
        params["flow"] = XUI_FLOW
    query = urlencode(params)
    display_name = build_vless_display_name()
    return f"vless://{user_uuid}@{SERVER_IP}:{SERVER_PORT}?{query}#{quote(display_name, safe='')}"


def build_vless_display_name() -> str:
    prefix = str(SERVER_NODE_PREFIX or "").strip()
    flag = str(SERVER_FLAG_EMOJI or "").strip()
    country = str(SERVER_COUNTRY or "").strip()

    if flag and country and country.startswith(flag):
        country_block = country
    else:
        country_block = " ".join(part for part in (flag, country) if part)
    if prefix and country_block:
        return f"{prefix} — {country_block} ⚡"
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


async def refresh_ticket_for_admins(ticket_id: int, assigned_label: str | None = None) -> None:
    ticket = get_support_ticket(ticket_id)
    if not ticket:
        return
    if not ADMIN_TELEGRAM_IDS:
        return

    text = support_ticket_admin_text(ticket, assigned_label=assigned_label)
    keyboard = build_ticket_admin_keyboard(ticket_id)
    message_ids = SUPPORT_TICKET_ADMIN_MESSAGE_IDS.setdefault(ticket_id, {})

    for admin_id in ADMIN_TELEGRAM_IDS:
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
        )
        add_support_ticket_message(
            ticket_id=ticket_id,
            sender_role="system",
            sender_id=admin_id,
            message_text=f"ticket_taken_by:{admin_id}",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ticket] Failed to notify user about taken ticket {ticket_id}: {exc}")


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


async def notify_admins_paid_order(
    payment: sqlite3.Row,
    user: sqlite3.Row | None,
    new_end: str,
) -> None:
    if not ADMIN_TELEGRAM_IDS:
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

    for admin_id in sorted(ADMIN_TELEGRAM_IDS):
        try:
            await bot.send_message(int(admin_id), text, parse_mode="HTML")
        except Exception as exc:  # noqa: BLE001
            print(f"[admin-notify] Failed to notify admin {admin_id}: {exc}")


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
    conn = get_conn()
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
        _extract_nested(payload, "metadata", "order_id"),
        _extract_nested(payload, "metadata", "invoice_id"),
        _extract_nested(payload, "data", "order_id"),
        _extract_nested(payload, "data", "invoice_id"),
        _extract_nested(payload, "data", "metadata", "order_id"),
        _extract_nested(payload, "data", "metadata", "invoice_id"),
        _extract_nested(payload, "data", "vars", "order_id"),
        _extract_nested(payload, "data", "vars", "invoice_id"),
    ]

    for value in candidates:
        if value:
            text = str(value).strip()
            if text:
                if text.upper().startswith("BV-"):
                    return text.upper()
                return text

    text_fields = [
        _extract_nested(payload, "message"),
        _extract_nested(payload, "comment"),
        _extract_nested(payload, "data", "message"),
        _extract_nested(payload, "data", "comment"),
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
        _extract_nested(payload, "data", "amount"),
        _extract_nested(payload, "data", "amount_main"),
        _extract_nested(payload, "data", "sum"),
        _extract_nested(payload, "data", "vars", "amount"),
        _extract_nested(payload, "data", "vars", "sum"),
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
        for word in ("wait", "pending", "process", "created", "new", "hold", "in_progress")
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
            return False, f"amount_mismatch:{incoming_amount}<{expected}"

        telegram_id = int(payment["telegram_id"])
        new_end = update_user_subscription(telegram_id, int(payment["days"]))
        mark_payment_paid(order_id, payload)
        try:
            consume_promocode_for_paid_order(order_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[promo] Failed to consume promo for {order_id}: {exc}")

        user = get_user(telegram_id)
        if not user:
            return False, "user_not_found"

        try:
            await notify_admins_paid_order(payment, user, new_end)
        except Exception as exc:  # noqa: BLE001
            print(f"[admin-notify] Failed to process paid notify for {order_id}: {exc}")

        notify_text = ""
        notify_parse_mode: str | None = None
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

    data_block = payload.get("data")
    if isinstance(data_block, dict) and isinstance(data_block.get("vars"), str):
        try:
            data_block["vars"] = json.loads(data_block["vars"])
        except json.JSONDecodeError:
            pass

    return payload


def validate_donationalerts_webhook_secret(request: web.Request, payload: dict[str, Any]) -> bool:
    if not DONATIONALERTS_WEBHOOK_SECRET:
        return True

    incoming = (
        request.headers.get("X-Webhook-Secret")
        or request.headers.get("X-DonationAlerts-Secret")
        or request.query.get("secret")
        or str(payload.get("secret") or "")
    )
    return hmac.compare_digest(str(incoming), DONATIONALERTS_WEBHOOK_SECRET)


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


async def donationalerts_webhook(request: web.Request) -> web.Response:
    try:
        payload = await parse_webhook_payload(request)
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"ok": False, "error": f"invalid_payload:{exc}"}, status=400)

    if not validate_donationalerts_webhook_secret(request, payload):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    if not is_successful_payment(payload):
        return web.json_response({"ok": True, "ignored": "not_success_event"})

    order_id, match_reason = resolve_order_id_from_payload(payload)
    if order_id and match_reason != "order_id":
        print(f"[webhook] Pending order matched by {match_reason}: {order_id}")

    if not order_id:
        return web.json_response({"ok": False, "error": "order_id_not_found"}, status=400)

    ok, reason = await process_paid_order(order_id, payload)
    code = 200 if ok else 400
    return web.json_response({"ok": ok, "reason": reason}, status=code)


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


async def donationalerts_oauth_callback(request: web.Request) -> web.Response:
    if not donationalerts_oauth_configured():
        return web.Response(
            text="DonationAlerts OAuth is not configured on server.",
            status=500,
            content_type="text/plain",
        )

    oauth_error = request.query.get("error")
    if oauth_error:
        return web.Response(
            text=f"OAuth error: {oauth_error}",
            status=400,
            content_type="text/plain",
        )

    expected_state = donationalerts_oauth_state()
    incoming_state = request.query.get("state", "")
    if expected_state and not hmac.compare_digest(incoming_state, expected_state):
        return web.Response(
            text="OAuth state mismatch.",
            status=401,
            content_type="text/plain",
        )

    code = request.query.get("code", "")
    if not code:
        return web.Response(
            text="Missing OAuth code.",
            status=400,
            content_type="text/plain",
        )

    try:
        token = await exchange_donationalerts_code(code)
        await set_donationalerts_token(token)
    except Exception as exc:  # noqa: BLE001
        return web.Response(
            text=f"Token exchange failed: {exc}",
            status=500,
            content_type="text/plain",
        )

    return web.Response(
        text="DonationAlerts connected successfully. You can close this page.",
        content_type="text/plain",
    )


def _is_sync_authorized(request: web.Request) -> bool:
    if not DONATIONALERTS_SYNC_SECRET:
        return True
    incoming = request.query.get("secret", "")
    return hmac.compare_digest(incoming, DONATIONALERTS_SYNC_SECRET)


async def donationalerts_oauth_url(request: web.Request) -> web.Response:
    if not _is_sync_authorized(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    if not donationalerts_oauth_configured():
        return web.json_response({"ok": False, "error": "oauth_not_configured"}, status=503)

    oauth_url = build_donationalerts_oauth_url()
    raw = str(request.query.get("raw", "")).lower() in {"1", "true", "yes"}
    if raw:
        return web.json_response({"ok": True, "url": oauth_url})
    raise web.HTTPFound(oauth_url)


async def donationalerts_sync(request: web.Request) -> web.Response:
    if not _is_sync_authorized(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    if not donationalerts_oauth_configured():
        return web.json_response({"ok": False, "error": "oauth_not_configured"}, status=503)

    access_token = await get_donationalerts_access_token()
    if not access_token:
        return web.json_response(
            {
                "ok": False,
                "error": "oauth_token_missing",
                "oauth_url": build_donationalerts_oauth_url(),
            },
            status=503,
        )

    try:
        stats = await process_donationalerts_sync(access_token)
    except PermissionError:
        refreshed = await get_donationalerts_access_token(force_refresh=True)
        if not refreshed:
            return web.json_response(
                {
                    "ok": False,
                    "error": "oauth_unauthorized",
                    "oauth_url": build_donationalerts_oauth_url(),
                },
                status=401,
            )
        stats = await process_donationalerts_sync(refreshed)

    return web.json_response(
        {
            "ok": True,
            "checked": stats["checked"],
            "matched": stats["matched"],
            "processed": stats["processed"],
            "last_donation_id": DA_LAST_DONATION_ID,
        }
    )


async def healthcheck(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "boxvolt-bot"})


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
    status_text = "✅ Active" if active else "❌ Inactive"
    status_hint = (
        "Подписка работает и будет обновляться автоматически."
        if active
        else "Подписка неактивна. Продлите доступ в боте."
    )
    support_url = f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}" if SUPPORT_CONTACT else ""
    username_text = f"@{username}" if username else str(telegram_id)
    country_text = server_country_label()
    sub_url_safe = html.escape(subscription_url)
    support_link_html = ""
    if support_url:
        support_link_html = (
            f'<a class="btn ghost" href="{html.escape(support_url)}" target="_blank" rel="noopener">'
            "Поддержка</a>"
        )
    active_class = "ok" if active else "bad"

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BoxVolt Технология 3.0</title>
  <style>
    :root {{
      --bg: #0a1020;
      --card: #121d33;
      --muted: #9fb0d1;
      --text: #eef4ff;
      --line: #2b3e67;
      --ok: #34d399;
      --bad: #f87171;
      --accent: #60a5fa;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--text);
      background:
        radial-gradient(1000px 500px at 10% -10%, #1f3b73 0%, transparent 60%),
        radial-gradient(900px 500px at 100% 0%, #162f5e 0%, transparent 50%),
        var(--bg);
      min-height: 100vh;
      padding: 20px;
    }}
    .wrap {{ max-width: 760px; margin: 0 auto; }}
    .card {{
      background: linear-gradient(180deg, rgba(22,34,58,.98), rgba(12,20,36,.98));
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 10px 30px rgba(0,0,0,.28);
    }}
    .title {{ font-size: 24px; margin: 0 0 6px; }}
    .subtitle {{ margin: 0 0 14px; color: var(--muted); }}
    .state {{
      display: inline-block;
      font-weight: 700;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      margin-bottom: 14px;
    }}
    .state.ok {{ color: var(--ok); border-color: rgba(52,211,153,.45); }}
    .state.bad {{ color: var(--bad); border-color: rgba(248,113,113,.45); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }}
    .item {{
      background: rgba(10,16,30,.55);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
    }}
    .k {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .v {{ font-size: 15px; }}
    .hint {{
      border-left: 3px solid var(--accent);
      background: rgba(96,165,250,.1);
      padding: 10px 12px;
      border-radius: 10px;
      margin-bottom: 14px;
      color: #dbeafe;
    }}
    .suburl {{
      width: 100%;
      background: #0a1428;
      color: #dbeafe;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font-size: 13px;
      margin-bottom: 10px;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .btn {{
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      color: #07111f;
      background: #67e8f9;
    }}
    .btn.ghost {{
      background: transparent;
      color: var(--text);
      border: 1px solid var(--line);
    }}
    .steps {{
      margin-top: 16px;
      border-top: 1px solid var(--line);
      padding-top: 12px;
    }}
    .steps h3 {{ margin: 0 0 10px; font-size: 17px; }}
    .steps ol {{ margin: 0; padding-left: 18px; color: var(--muted); }}
    .steps li {{ margin: 6px 0; }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="card">
      <h1 class="title">BoxVolt Технология 3.0</h1>
      <p class="subtitle">Профиль подписки Happ / V2rayTun</p>

      <div class="state {active_class}">{status_text}</div>

      <div class="grid">
        <div class="item"><div class="k">Пользователь</div><div class="v">{html.escape(username_text)}</div></div>
        <div class="item"><div class="k">Telegram ID</div><div class="v">{telegram_id}</div></div>
        <div class="item"><div class="k">Сервер</div><div class="v">{html.escape(country_text)}</div></div>
        <div class="item"><div class="k">Истекает</div><div class="v">{html.escape(expiry_text)}</div></div>
        <div class="item"><div class="k">Осталось</div><div class="v">{html.escape(remaining_text)}</div></div>
        <div class="item"><div class="k">Автообновление</div><div class="v">Каждые {SUBSCRIPTION_UPDATE_INTERVAL_HOURS} ч.</div></div>
      </div>

      <div class="hint">{html.escape(status_hint)}</div>

      <input id="subUrl" class="suburl" value="{sub_url_safe}" readonly>
      <div class="actions">
        <button class="btn" type="button" onclick="copySubUrl()">Скопировать подписку</button>
        {support_link_html}
      </div>

      <div class="steps">
        <h3>Рекомендуемое приложение: Happ</h3>
        <ol>
          <li>Установите Happ и запустите приложение.</li>
          <li>Добавьте подписку по URL (поле выше).</li>
          <li>Выберите профиль BoxVolt и нажмите Connect.</li>
        </ol>
      </div>
    </section>
  </main>
  <script>
    async function copySubUrl() {{
      const input = document.getElementById("subUrl");
      const text = input.value || "";
      try {{
        if (navigator.clipboard && navigator.clipboard.writeText) {{
          await navigator.clipboard.writeText(text);
        }} else {{
          input.select();
          document.execCommand("copy");
        }}
        alert("URL подписки скопирован");
      }} catch (e) {{
        alert("Не удалось скопировать. Скопируйте вручную.");
      }}
    }}
  </script>
</body>
</html>
"""


async def subscription_feed(request: web.Request) -> web.Response:
    telegram_id_raw = str(request.match_info.get("telegram_id") or "").strip()
    token = str(request.match_info.get("token") or "").strip()

    if not telegram_id_raw.isdigit():
        return web.Response(text="bad_telegram_id", status=400)

    telegram_id = int(telegram_id_raw)
    if not is_valid_subscription_token(telegram_id, token):
        return web.Response(text="forbidden", status=403)

    user = get_user(telegram_id)
    if not user:
        return web.Response(text="user_not_found", status=404)
    if not has_active_subscription(user["subscription_end"]):
        return web.Response(text="subscription_expired", status=403)

    try:
        user_uuid = await ensure_vless_uuid(
            telegram_id,
            user["vless_uuid"],
            user["subscription_end"],
        )
        vless_link = await generate_vless_link(user_uuid)
    except Exception as exc:  # noqa: BLE001
        return web.Response(text=f"key_generation_failed: {exc}", status=500)

    headers = {
        "Cache-Control": "no-store",
        "profile-title": build_profile_title_header(),
        "profile-update-interval": str(SUBSCRIPTION_UPDATE_INTERVAL_HOURS),
        "subscription-userinfo": (
            f"upload=0; download=0; total={subscription_total_bytes()}; "
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
        text=f"{vless_link}\n",
        headers=headers,
        content_type="text/plain",
    )


async def subscription_profile_page(request: web.Request) -> web.Response:
    telegram_id_raw = str(request.match_info.get("telegram_id") or "").strip()
    token = str(request.match_info.get("token") or "").strip()

    if not telegram_id_raw.isdigit():
        return web.Response(text="bad_telegram_id", status=400)

    telegram_id = int(telegram_id_raw)
    if not is_valid_subscription_token(telegram_id, token):
        return web.Response(text="forbidden", status=403)

    user = get_user(telegram_id)
    if not user:
        return web.Response(text="user_not_found", status=404)

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

    pending_order = None
    if pending:
        pending_order_id = str(pending["order_id"])
        pending_amount = float(pending["amount_rub"])
        promo_code = str(pending["promo_code"] or "").strip()
        promo_discount = int(pending["promo_discount_rub"] or 0)
        pending_provider = normalize_payment_provider(pending["provider"])
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
            "payment_url": build_payment_url(
                pending_order_id,
                int(round(pending_amount)),
                pending_provider,
            ),
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
    payment_url = build_payment_url(order_id, final_amount, provider)
    order_plan = serialize_plan(plan)
    order_plan["amount_rub"] = final_amount
    order_plan["base_amount_rub"] = plan.amount_rub
    order_plan["promo"] = promo

    return web.json_response(
        {
            "ok": True,
            "order_id": order_id,
            "payment_url": payment_url,
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

    da_webhook_path = _normalize_http_path(DONATIONALERTS_WEBHOOK_PATH, "/donationalerts/webhook")
    donatepay_webhook_path = _normalize_http_path(DONATEPAY_WEBHOOK_PATH, "/donatepay/webhook")
    oauth_callback_path = _normalize_http_path(
        DONATIONALERTS_OAUTH_CALLBACK_PATH,
        "/donationalerts/oauth/callback",
    )
    oauth_url_path = _normalize_http_path(
        DONATIONALERTS_OAUTH_URL_PATH,
        "/donationalerts/oauth/url",
    )
    sync_path = _normalize_http_path(DONATIONALERTS_SYNC_PATH, "/donationalerts/sync")
    subscription_path = _normalize_http_path(SUBSCRIPTION_PATH, "/sub").rstrip("/")
    if not subscription_path:
        subscription_path = "/sub"

    app.router.add_post(da_webhook_path, donationalerts_webhook)
    app.router.add_post(donatepay_webhook_path, donatepay_webhook)
    app.router.add_get(oauth_callback_path, donationalerts_oauth_callback)
    app.router.add_get(oauth_url_path, donationalerts_oauth_url)
    app.router.add_get(sync_path, donationalerts_sync)
    app.router.add_get(f"{subscription_path}/{{telegram_id}}/{{token}}/profile", subscription_profile_page)
    app.router.add_get(f"{subscription_path}/{{telegram_id}}/{{token}}", subscription_feed)
    app.router.add_get("/health", healthcheck)
    app.router.add_get("/webapp", webapp_page)
    app.router.add_get("/webapp/", webapp_page)
    app.router.add_get("/webapp/api/plans", webapp_plans_api)
    app.router.add_post("/webapp/api/me", webapp_me_api)
    app.router.add_post("/webapp/api/create-order", webapp_create_order_api)
    app.router.add_post("/webapp/api/order-status", webapp_order_status_api)
    app.router.add_post("/webapp/api/activate-promocode", webapp_activate_promocode_api)
    app.router.add_post("/webapp/api/cancel-order", webapp_cancel_order_api)
    app.router.add_post("/webapp/api/admin/pricing", webapp_admin_pricing_api)
    app.router.add_post("/webapp/api/admin/save-pricing", webapp_admin_save_pricing_api)
    app.router.add_post("/webapp/api/admin/notify", webapp_admin_notify_api)
    app.router.add_post("/webapp/api/admin/promocodes", webapp_admin_promocodes_api)
    app.router.add_post("/webapp/api/admin/create-promocode", webapp_admin_create_promocode_api)
    app.router.add_post("/webapp/api/admin/find-user", webapp_admin_find_user_api)
    app.router.add_post("/webapp/api/admin/grant-subscription", webapp_admin_grant_subscription_api)
    app.router.add_post("/webapp/api/admin/remove-subscription", webapp_admin_remove_subscription_api)
    return app


@dp.message(Command("start"))
async def start_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)

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
    )

    await message.answer(intro, reply_markup=build_main_keyboard(message.from_user.id))


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
    await message.answer(f"Ваш Telegram ID: {message.from_user.id}")


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
        "🛠 Админ-панель\n"
        "Команды:\n"
        "• /prices — текущие цены и скидки\n"
        "• /sale_notify — рассылка акции из pricing.json\n"
        "• /sale_notify <текст> — рассылка своего текста\n"
        "• /promo CODE — проверить активацию промокода как пользователь\n"
        "• /myid — показать ваш Telegram ID\n\n"
        "Промокоды создаются в Mini App админ-панели.\n"
        f"Авто-отмена неоплаченных заказов: {PAYMENT_PENDING_TTL_MINUTES} минут."
    )


@dp.message(Command("admin"))
async def admin_panel_command_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await show_admin_panel(message)


@dp.message(Command("webapp"))
async def open_webapp_handler(message: Message) -> None:
    if not WEBAPP_PUBLIC_URL:
        await message.answer("❌ WebApp не настроен. Укажите `WEBAPP_PUBLIC_URL` в .env.")
        return

    await message.answer(f"🧩 Откройте Mini App: {WEBAPP_PUBLIC_URL}")


@dp.message(F.text == "🧩 Mini App")
async def mini_app_text_handler(message: Message) -> None:
    if not WEBAPP_PUBLIC_URL:
        await message.answer("❌ WebApp не настроен. Укажите `WEBAPP_PUBLIC_URL` в .env.")
        return

    await message.answer(f"🧩 Ссылка на Mini App: {WEBAPP_PUBLIC_URL}")


@dp.message(F.text == "💳 Купить подписку")
async def buy_menu_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)

    active_provider = get_active_payment_provider()
    if not payment_provider_is_ready(active_provider):
        missing = (
            "`DONATEPAY_DONATE_BASE_URL` и `DONATEPAY_API_KEY`"
            if active_provider == DONATEPAY_PROVIDER
            else "`DONATIONALERTS_USERNAME`"
        )
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
    payment_url = build_payment_url(order_id, final_amount, provider)

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
    if WEBAPP_PUBLIC_URL:
        pay_step_line = (
            "1. Нажмите «Оплатить в Mini App» (откроется Telegram-окно оплаты).\n"
        )

    text = (
        f"🧾 Заказ создан: {order_id}\n"
        f"Тариф: {plan.title}\n"
        f"{amount_line}\n\n"
        "Как оплатить:\n"
        f"{pay_step_line}"
        f"2. В {provider_label} укажите сумму ровно: {final_amount} ₽\n"
        f"3. Если есть поле комментария, вставьте код: {order_id}\n"
        "4. После оплаты нажмите «Проверить оплату».\n\n"
        f"⏱ Неоплаченный заказ отменится автоматически через {PAYMENT_PENDING_TTL_MINUTES} минут."
    )
    text = apply_order_status_to_text(text, "⏳ Ожидаем оплату.")

    await callback.message.answer(
        text,
        reply_markup=build_payment_keyboard(payment_url, order_id, provider),
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
        keyboard = build_order_closed_keyboard()
        answer_text = "Оплата подтверждена"
    elif payment["status"] == "cancelled":
        status_text = "❌ Заказ отменен или просрочен. Создайте новый через «Купить подписку»."
        keyboard = build_order_closed_keyboard()
        answer_text = "Заказ отменен"
    else:
        expires_at = payment_expires_at_str(payment["created_at"]) or "-"
        status_text = (
            "⏳ Платеж еще не подтвержден.\n"
            f"Заказ автоматически отменится в: {expires_at}."
        )
        amount_rub = int(round(float(payment["amount_rub"])))
        keyboard = build_payment_keyboard(
            build_payment_url(order_id, amount_rub, payment["provider"]),
            order_id,
            payment["provider"],
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
                reply_markup=build_order_closed_keyboard(),
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
        f"🌍 Страна сервера: {server_country_label()}\n"
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


@dp.message(F.text == "🛟 Поддержка")
async def support_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)

    if not ADMIN_TELEGRAM_IDS:
        await message.answer(
            "🛟 Поддержка\n"
            f"Сейчас тикеты недоступны. Напишите напрямую: {SUPPORT_CONTACT}"
        )
        return

    SUPPORT_WAITING_USERS.add(message.from_user.id)
    await message.answer(
        "🛟 Поддержка\n"
        "Опишите проблему одним сообщением.\n"
        "Администраторы увидят тикет и ответят здесь в боте.\n\n"
        "Для отмены отправьте: /cancel"
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
        await notify_admins_ticket_taken(ticket_id, callback.from_user.id, taker_label)
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
        await notify_admins_ticket_taken(ticket_id, callback.from_user.id, taker_label)

    ADMIN_REPLY_TICKET_BY_ADMIN[callback.from_user.id] = ticket_id
    await callback.answer("Отправьте следующим сообщением ответ пользователю", show_alert=False)
    await callback.message.answer(
        f"💬 Режим ответа на тикет #{ticket_id} включен.\n"
        "Следующее ваше сообщение уйдет пользователю.\n"
        "Отмена: /cancel"
    )


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

    ok, reason = close_support_ticket(ticket_id, callback.from_user.id)
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
        try:
            await bot.send_message(
                int(ticket["telegram_id"]),
                f"✅ Тикет #{ticket_id} закрыт администратором.\n"
                "Если вопрос остался, создайте новый тикет через «🛟 Поддержка».",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[ticket] Failed to notify user about closed ticket {ticket_id}: {exc}")

    await refresh_ticket_for_admins(ticket_id)
    for admin_id, reply_ticket_id in list(ADMIN_REPLY_TICKET_BY_ADMIN.items()):
        if reply_ticket_id == ticket_id:
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(admin_id, None)

    await callback.answer("Тикет закрыт", show_alert=False)


@dp.message(F.text == "🛠 Админ")
async def admin_panel_text_handler(message: Message) -> None:
    if not await ensure_admin(message):
        return
    await show_admin_panel(message)


@dp.message(F.text == "🔥 Акции")
async def sale_handler(message: Message) -> None:
    sale_text = get_sale_text()
    if sale_text:
        await message.answer(sale_text)
        return
    await message.answer("Сейчас активных скидок нет. Следите за обновлениями 👀")


async def handle_trial_request(message: Message) -> None:
    if not TRIAL_ENABLED:
        await message.answer("Тестовый период отключен.")
        return

    upsert_user(message.from_user.id, message.from_user.username)
    user = get_user(message.from_user.id)
    if not user:
        await message.answer("Пользователь не найден. Нажмите /start")
        return

    if int(user["trial_used"] or 0) == 1:
        await message.answer("❌ Тест уже использован.")
        return

    if has_trial_claim(message.from_user.id):
        await message.answer("❌ Тест уже использован.")
        return

    if user_has_paid_payment(message.from_user.id):
        await message.answer("❌ Тест доступен только до первой оплаты.")
        return

    if parse_date(user["subscription_end"]):
        await message.answer("❌ Тест уже был активирован ранее.")
        return

    conn = get_conn()
    inserted_trial_claim = conn.execute(
        """
        INSERT OR IGNORE INTO trial_claims (telegram_id, username, claimed_at)
        VALUES (?, ?, ?)
        """,
        (message.from_user.id, message.from_user.username, now_str()),
    )
    if int(inserted_trial_claim.rowcount or 0) == 0:
        conn.close()
        await message.answer("❌ Тест уже использован.")
        return

    end_at = dt.datetime.now() + dt.timedelta(days=TRIAL_DAYS)
    conn.execute(
        """
        UPDATE users
        SET trial_used = 1,
            subscription_end = ?
        WHERE telegram_id = ?
        """,
        (end_at.strftime("%Y-%m-%d %H:%M:%S"), message.from_user.id),
    )
    conn.commit()
    conn.close()

    fresh_user = get_user(message.from_user.id)
    if not fresh_user:
        await message.answer("✅ Тест активирован.")
        return

    try:
        await ensure_vless_uuid(
            message.from_user.id,
            fresh_user["vless_uuid"],
            fresh_user["subscription_end"],
        )
        subscription_block = build_subscription_text_block(message.from_user.id)
        if not subscription_block:
            raise RuntimeError("subscription_url_not_configured")
        await message.answer(
            f"✅ Тест активирован на {TRIAL_DAYS} дня.\n"
            f"До: {fresh_user['subscription_end']}\n\n"
            "🔑 Ваш доступ:\n"
            f"{subscription_block}",
            parse_mode="HTML",
        )
    except Exception as exc:  # noqa: BLE001
        await message.answer(
            f"✅ Тест активирован на {TRIAL_DAYS} дня.\n"
            "⚠️ URL-подписку не удалось выдать автоматически.\n"
            "Нажмите «🚀 Подключить VPN» или обратитесь в поддержку.\n"
            f"Техническая ошибка: {exc}"
        )


@dp.message(Command("trial"))
async def trial_command_handler(message: Message) -> None:
    await handle_trial_request(message)


@dp.message(F.text.startswith("🎁 Тест на "))
async def trial_handler(message: Message) -> None:
    await handle_trial_request(message)


@dp.message(Command("cancel"))
async def cancel_context_handler(message: Message) -> None:
    cancelled = False

    if message.from_user.id in PROMO_WAITING_USERS:
        PROMO_WAITING_USERS.discard(message.from_user.id)
        cancelled = True

    if message.from_user.id in SUPPORT_WAITING_USERS:
        SUPPORT_WAITING_USERS.discard(message.from_user.id)
        cancelled = True

    if message.from_user.id in ADMIN_REPLY_TICKET_BY_ADMIN:
        ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
        cancelled = True

    if cancelled:
        await message.answer("Отменено.")
        return

    await message.answer("Нет активного действия для отмены.")


@dp.message(F.text)
async def text_context_handler(message: Message) -> None:
    text = (message.text or "").strip()
    if not text:
        return

    # Режим ответа администратора на выбранный тикет.
    if is_admin_user(message.from_user.id) and message.from_user.id in ADMIN_REPLY_TICKET_BY_ADMIN:
        if text.lower() in {"отмена", "/cancel"}:
            ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id, None)
            await message.answer("Ответ на тикет отменен.")
            return

        ticket_id = ADMIN_REPLY_TICKET_BY_ADMIN.pop(message.from_user.id)
        ticket, was_taken_now = ensure_support_ticket_in_progress(ticket_id, message.from_user.id)
        if not ticket:
            await message.answer("Тикет не найден.")
            return
        if str(ticket["status"]) == "closed":
            await message.answer("Тикет уже закрыт.")
            return
        assigned_admin_id = ticket["assigned_admin_id"]
        if assigned_admin_id and int(assigned_admin_id) != message.from_user.id:
            await message.answer(f"Тикет уже ведет {admin_label(int(assigned_admin_id))}.")
            return
        if was_taken_now:
            taker_label = admin_label(message.from_user.id, message.from_user.username)
            await notify_user_ticket_taken(ticket_id, message.from_user.id)
            await refresh_ticket_for_admins(ticket_id, assigned_label=taker_label)
            await notify_admins_ticket_taken(ticket_id, message.from_user.id, taker_label)

        try:
            await bot.send_message(
                int(ticket["telegram_id"]),
                f"💬 Ответ администратора по тикету #{ticket_id}:\n{text}",
            )
            add_support_ticket_message(
                ticket_id=ticket_id,
                sender_role="admin",
                sender_id=message.from_user.id,
                message_text=text,
            )
            await message.answer("Ответ отправлен пользователю.")
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
        await notify_admins_new_ticket(ticket_id)
        await message.answer(
            f"✅ Тикет #{ticket_id} создан.\n"
            "Ожидайте: администратор возьмет тикет и ответит в этом чате."
        )
        return


async def start_webhook_server() -> web.AppRunner:
    app = make_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_HOST, WEBHOOK_PORT)
    await site.start()

    da_webhook_path = _normalize_http_path(DONATIONALERTS_WEBHOOK_PATH, "/donationalerts/webhook")
    donatepay_webhook_path = _normalize_http_path(DONATEPAY_WEBHOOK_PATH, "/donatepay/webhook")
    oauth_callback_path = _normalize_http_path(
        DONATIONALERTS_OAUTH_CALLBACK_PATH,
        "/donationalerts/oauth/callback",
    )
    oauth_url_path = _normalize_http_path(
        DONATIONALERTS_OAUTH_URL_PATH,
        "/donationalerts/oauth/url",
    )
    sync_path = _normalize_http_path(DONATIONALERTS_SYNC_PATH, "/donationalerts/sync")
    subscription_path = _normalize_http_path(SUBSCRIPTION_PATH, "/sub").rstrip("/")
    if not subscription_path:
        subscription_path = "/sub"

    print(f"[webhook] DonationAlerts path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{da_webhook_path}")
    print(f"[webhook] DonatePay path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{donatepay_webhook_path}")
    print(f"[da-oauth] Callback path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{oauth_callback_path}")
    print(f"[da-sync] OAuth URL path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{oauth_url_path}")
    print(f"[da-sync] Manual sync path http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{sync_path}")
    print(f"[webapp] Internal URL http://{WEBHOOK_HOST}:{WEBHOOK_PORT}/webapp")
    print(f"[pricing] File {pricing_path()}")
    if WEBAPP_PUBLIC_URL:
        print(f"[webapp] Public URL {WEBAPP_PUBLIC_URL}")
    public_base = resolved_public_base_url()
    if public_base:
        print(f"[sub] Public pattern {public_base}{subscription_path}/<telegram_id>/<token>")
    if donationalerts_oauth_configured() and not load_donationalerts_token():
        print("[da-oauth] No saved token. Open authorize URL once:")
        print(f"[da-oauth] {build_donationalerts_oauth_url()}")
    return runner


async def main() -> None:
    ensure_pricing_file_exists()
    init_db()
    await maybe_send_update_notification()
    webhook_runner = await start_webhook_server()
    poll_tasks: list[asyncio.Task[None]] = []
    cleanup_task: asyncio.Task[None] | None = None
    if DONATIONALERTS_POLL_ENABLED and (
        get_active_payment_provider() == "donationalerts"
        or donationalerts_oauth_configured()
        or bool(load_donationalerts_token())
    ):
        poll_tasks.append(asyncio.create_task(donationalerts_poll_loop(), name="da-poll-loop"))
    if DONATEPAY_POLL_ENABLED and DONATEPAY_API_KEY and (
        get_active_payment_provider() == DONATEPAY_PROVIDER
        or has_pending_orders_for_provider(DONATEPAY_PROVIDER)
    ):
        poll_tasks.append(asyncio.create_task(donatepay_poll_loop(), name="dp-poll-loop"))
    cleanup_task = asyncio.create_task(payments_cleanup_loop(), name="payments-cleanup-loop")
    try:
        await dp.start_polling(bot)
    finally:
        for task in poll_tasks:
            task.cancel()
        for task in poll_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if cleanup_task:
            cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cleanup_task
        await webhook_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
