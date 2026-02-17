import asyncio
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

import httpx
from aiohttp import web
from aiogram import Bot, Dispatcher, F
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


BOT_TOKEN = env("BOT_TOKEN", required=True)
DB_PATH = env("DB_PATH", str(BASE_DIR / "users.db"))

# 3x-ui / VLESS
XUI_URL = env("XUI_URL", "http://185.23.19.82:2053").rstrip("/")
XUI_USERNAME = env("XUI_USERNAME", "")
XUI_PASSWORD = env("XUI_PASSWORD", "")
INBOUND_ID = int(env("INBOUND_ID", "1"))
SERVER_IP = env("SERVER_IP", "185.23.19.82")
SERVER_PORT = int(env("SERVER_PORT", "443"))
PUBLIC_KEY = env("PUBLIC_KEY", "")
SHORT_ID = env("SHORT_ID", "")
SNI = env("SNI", "www.cloudflare.com")
UTLS_FP = env("UTLS_FP", "chrome")

# DonationAlerts
DONATIONALERTS_USERNAME = env("DONATIONALERTS_USERNAME", "")
DONATIONALERTS_WEBHOOK_SECRET = env("DONATIONALERTS_WEBHOOK_SECRET", "")
DONATIONALERTS_WEBHOOK_PATH = env("DONATIONALERTS_WEBHOOK_PATH", "/donationalerts/webhook")

# Bot settings
SUPPORT_CONTACT = env("SUPPORT_CONTACT", "@boxvolt_support")
TRIAL_ENABLED = env("TRIAL_ENABLED", "0") == "1"
TRIAL_DAYS = int(env("TRIAL_DAYS", "3"))
WEBAPP_PUBLIC_URL = env("WEBAPP_PUBLIC_URL", "")
WEBAPP_INITDATA_MAX_AGE_SECONDS = int(env("WEBAPP_INITDATA_MAX_AGE_SECONDS", "86400"))

# Web server for webhook
WEBHOOK_HOST = env("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT = int(env("WEBHOOK_PORT", "8080"))

PLANS: dict[str, Plan] = {
    "m1": Plan(
        code="m1",
        title="30 дней",
        days=int(env("PLAN_30_DAYS", "30")),
        amount_rub=int(env("PLAN_30_AMOUNT_RUB", "50")),
    ),
    "m3": Plan(
        code="m3",
        title="90 дней",
        days=int(env("PLAN_90_DAYS", "90")),
        amount_rub=int(env("PLAN_90_AMOUNT_RUB", "120")),
    ),
}

GUIDES: dict[str, tuple[str, str]] = {
    "android_happ": (
        "🤖 Android • Happ",
        "1. Установите Happ из Google Play.\n"
        "2. Откройте приложение и выберите импорт по ссылке.\n"
        "3. Вставьте ваш VLESS-ключ из бота.\n"
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
        "3. Импортируйте VLESS-ссылку из бота.\n"
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
        "2. Импортируйте ссылку VLESS из бота.\n"
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
PROCESS_LOCK = asyncio.Lock()
WEBAPP_TEMPLATE_PATH = BASE_DIR / "webapp" / "index.html"

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


def create_payment_order(telegram_id: int, plan: Plan) -> str:
    order_id = f"BV-{telegram_id}-{uuid.uuid4().hex[:8].upper()}"
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO payments (order_id, telegram_id, provider, amount_rub, days, created_at)
        VALUES (?, ?, 'donationalerts', ?, ?, ?)
        """,
        (order_id, telegram_id, float(plan.amount_rub), plan.days, now_str()),
    )
    conn.commit()
    conn.close()
    return order_id


def get_payment(order_id: str) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT id, order_id, telegram_id, provider, amount_rub, days, status, created_at, paid_at
        FROM payments
        WHERE order_id = ?
        """,
        (order_id,),
    ).fetchone()
    conn.close()
    return row


def get_latest_pending_payment(telegram_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT order_id, amount_rub, days, created_at
        FROM payments
        WHERE telegram_id = ? AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (telegram_id,),
    ).fetchone()
    conn.close()
    return row


def build_main_keyboard() -> ReplyKeyboardMarkup:
    keyboard_rows: list[list[KeyboardButton]] = [
        [
            KeyboardButton(text="🚀 Подключить VPN"),
            KeyboardButton(text="💳 Купить подписку"),
        ],
        [KeyboardButton(text="👤 Личный кабинет"), KeyboardButton(text="📚 Инструкции")],
        [KeyboardButton(text="🛟 Поддержка")],
    ]

    if WEBAPP_PUBLIC_URL:
        keyboard_rows.append(
            [KeyboardButton(text="🧩 Mini App", web_app=WebAppInfo(url=WEBAPP_PUBLIC_URL))]
        )

    return ReplyKeyboardMarkup(
        keyboard=keyboard_rows,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие",
    )


def build_plan_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"{plan.title} • {plan.amount_rub} ₽",
                callback_data=f"buy:{plan.code}",
            )
        ]
        for plan in PLANS.values()
    ]
    rows.append([InlineKeyboardButton(text="📚 Как подключить", callback_data="guides:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_payment_keyboard(payment_url: str, order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💸 Оплатить в DonationAlerts", url=payment_url)],
            [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"paycheck:{order_id}")],
            [InlineKeyboardButton(text="📚 Инструкции", callback_data="guides:open")],
        ]
    )


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


def build_donation_url(order_id: str, amount_rub: int) -> str:
    base = f"https://www.donationalerts.com/r/{DONATIONALERTS_USERNAME}"
    params = {
        "amount": amount_rub,
        "currency": "RUB",
        "message": order_id,
    }
    return f"{base}?{urlencode(params)}"


def serialize_plan(plan: Plan) -> dict[str, Any]:
    return {
        "code": plan.code,
        "title": plan.title,
        "days": plan.days,
        "amount_rub": plan.amount_rub,
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


def generate_vless_link(user_uuid: str) -> str:
    params = {
        "encryption": "none",
        "type": "tcp",
        "security": "reality",
        "sni": SNI,
        "fp": UTLS_FP,
        "pbk": PUBLIC_KEY,
        "sid": SHORT_ID,
    }
    query = urlencode(params)
    return f"vless://{user_uuid}@{SERVER_IP}:{SERVER_PORT}?{query}#BoxVoltVPN"


def user_title(message: Message) -> str:
    username = message.from_user.username
    if username:
        return f"@{username}"
    return str(message.from_user.id)


async def create_vless_user(telegram_id: int) -> str:
    if not all([XUI_URL, XUI_USERNAME, XUI_PASSWORD, PUBLIC_KEY, SHORT_ID]):
        raise RuntimeError("3x-ui config is incomplete in .env")

    user_uuid = str(uuid.uuid4())
    settings = {
        "clients": [
            {
                "id": user_uuid,
                "flow": "",
                "email": str(telegram_id),
                "limitIp": 1,
                "totalGB": 0,
                "expiryTime": 0,
                "enable": True,
            }
        ]
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        login_resp = await client.post(
            f"{XUI_URL}/login",
            data={"username": XUI_USERNAME, "password": XUI_PASSWORD},
        )
        login_resp.raise_for_status()

        resp = await client.post(
            f"{XUI_URL}/panel/api/inbounds/addClient",
            data={"id": INBOUND_ID, "settings": json.dumps(settings)},
            cookies=login_resp.cookies,
        )
        resp.raise_for_status()

        try:
            body = resp.json()
        except json.JSONDecodeError:
            body = {}

        if isinstance(body, dict) and body.get("success") is False:
            raise RuntimeError(f"3x-ui addClient failed: {body}")

    return user_uuid


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

    if any(word in status for word in ("fail", "cancel", "reject", "decline")):
        return False

    if status in {"paid", "success", "succeeded", "completed"}:
        return True

    if any(word in event for word in ("donation", "paid", "success")):
        return True

    # Иногда DA присылает payload без status/event, но с суммой и данными доната.
    return extract_amount(payload) is not None


async def ensure_vless_uuid(telegram_id: int, existing_uuid: str | None) -> str:
    if existing_uuid:
        return existing_uuid
    new_uuid = await create_vless_user(telegram_id)
    save_user_uuid(telegram_id, new_uuid)
    return new_uuid


async def process_paid_order(order_id: str, payload: dict[str, Any]) -> tuple[bool, str]:
    async with PROCESS_LOCK:
        payment = get_payment(order_id)
        if not payment:
            return False, "order_not_found"

        if payment["status"] == "paid":
            return True, "already_paid"

        incoming_amount = extract_amount(payload)
        expected = float(payment["amount_rub"])

        if incoming_amount is not None and incoming_amount + 0.01 < expected:
            return False, f"amount_mismatch:{incoming_amount}<{expected}"

        telegram_id = int(payment["telegram_id"])
        new_end = update_user_subscription(telegram_id, int(payment["days"]))
        mark_payment_paid(order_id, payload)

        user = get_user(telegram_id)
        if not user:
            return False, "user_not_found"

        notify_text = ""
        try:
            user_uuid = await ensure_vless_uuid(telegram_id, user["vless_uuid"])
            link = generate_vless_link(user_uuid)
            notify_text = (
                "✅ Оплата подтверждена!\n\n"
                f"📅 Подписка активна до: {new_end}\n"
                "🔑 Ваш VLESS-ключ:\n"
                f"{link}\n\n"
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
                reply_markup=build_main_keyboard(),
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

    return payload


def validate_webhook_secret(request: web.Request, payload: dict[str, Any]) -> bool:
    if not DONATIONALERTS_WEBHOOK_SECRET:
        return True

    incoming = (
        request.headers.get("X-Webhook-Secret")
        or request.headers.get("X-DonationAlerts-Secret")
        or request.query.get("secret")
        or str(payload.get("secret") or "")
    )
    return incoming == DONATIONALERTS_WEBHOOK_SECRET


async def donationalerts_webhook(request: web.Request) -> web.Response:
    try:
        payload = await parse_webhook_payload(request)
    except Exception as exc:  # noqa: BLE001
        return web.json_response({"ok": False, "error": f"invalid_payload:{exc}"}, status=400)

    if not validate_webhook_secret(request, payload):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    if not is_successful_payment(payload):
        return web.json_response({"ok": True, "ignored": "not_success_event"})

    order_id = extract_order_id(payload)
    if not order_id:
        telegram_id = extract_telegram_id(payload)
        if telegram_id:
            pending = get_latest_pending_payment(telegram_id)
            if pending:
                order_id = pending["order_id"]

    if not order_id:
        return web.json_response({"ok": False, "error": "order_id_not_found"}, status=400)

    ok, reason = await process_paid_order(order_id, payload)
    code = 200 if ok else 400
    return web.json_response({"ok": ok, "reason": reason}, status=code)


async def healthcheck(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "boxvolt-bot"})


def webapp_error(error: str, status: int = 400) -> web.Response:
    return web.json_response({"ok": False, "error": error}, status=status)


def webapp_auth_error_status(error: str) -> int:
    if error in {"bad_signature", "init_data_expired"}:
        return 401
    return 400


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
    return web.json_response(
        {
            "ok": True,
            "payment_enabled": bool(DONATIONALERTS_USERNAME),
            "plans": [serialize_plan(plan) for plan in PLANS.values()],
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

    pending_order = None
    if pending:
        pending_order = {
            "order_id": pending["order_id"],
            "amount_rub": float(pending["amount_rub"]),
            "days": int(pending["days"]),
            "created_at": pending["created_at"],
        }

    return web.json_response(
        {
            "ok": True,
            "user": {
                "id": telegram_id,
                "username": username,
                "first_name": user_obj.get("first_name"),
                "last_name": user_obj.get("last_name"),
            },
            "subscription": {
                "active": has_active_subscription(user["subscription_end"] if user else None),
                "subscription_end": user["subscription_end"] if user else None,
            },
            "pending_order": pending_order,
        }
    )


async def webapp_create_order_api(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return webapp_error("invalid_json", 400)

    if not DONATIONALERTS_USERNAME:
        return webapp_error("payment_not_configured", 503)

    ok, data, reason = validate_webapp_init_data(str(body.get("init_data") or ""))
    if not ok or not data:
        return webapp_error(reason, webapp_auth_error_status(reason))

    plan_code = str(body.get("plan_code") or "")
    plan = PLANS.get(plan_code)
    if not plan:
        return webapp_error("unknown_plan", 400)

    user_obj = data["user_obj"]
    telegram_id = int(user_obj["id"])
    username = user_obj.get("username")
    upsert_user(telegram_id, username)

    order_id = create_payment_order(telegram_id, plan)
    payment_url = build_donation_url(order_id, plan.amount_rub)

    return web.json_response(
        {
            "ok": True,
            "order_id": order_id,
            "payment_url": payment_url,
            "plan": serialize_plan(plan),
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
    payment = get_payment(order_id)
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
                "days": int(payment["days"]),
                "paid_at": payment["paid_at"],
            },
            "subscription_end": user["subscription_end"] if user else None,
        }
    )


def make_web_app() -> web.Application:
    app = web.Application()

    path = DONATIONALERTS_WEBHOOK_PATH.strip()
    if not path.startswith("/"):
        path = f"/{path}"

    app.router.add_post(path, donationalerts_webhook)
    app.router.add_get("/health", healthcheck)
    app.router.add_get("/webapp", webapp_page)
    app.router.add_get("/webapp/", webapp_page)
    app.router.add_get("/webapp/api/plans", webapp_plans_api)
    app.router.add_post("/webapp/api/me", webapp_me_api)
    app.router.add_post("/webapp/api/create-order", webapp_create_order_api)
    app.router.add_post("/webapp/api/order-status", webapp_order_status_api)
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

    await message.answer(intro, reply_markup=build_main_keyboard())


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

    if not DONATIONALERTS_USERNAME:
        await message.answer(
            "⚠️ Оплата временно недоступна: не задан `DONATIONALERTS_USERNAME` в .env."
        )
        return

    pending = get_latest_pending_payment(message.from_user.id)
    pending_text = ""
    if pending:
        pending_text = (
            "\n\nНайден незавершенный заказ:\n"
            f"Код: {pending['order_id']}\n"
            f"Сумма: {pending['amount_rub']} ₽\n"
            "Можно оплатить его или создать новый."
        )

    text = (
        "💳 Выберите тариф.\n"
        "После выбора бот создаст код заказа и откроет DonationAlerts." + pending_text
    )
    await message.answer(text, reply_markup=build_plan_keyboard())


@dp.callback_query(F.data.startswith("buy:"))
async def buy_plan_callback(callback: CallbackQuery) -> None:
    if not callback.data:
        await callback.answer()
        return

    if not DONATIONALERTS_USERNAME:
        await callback.answer("Оплата не настроена", show_alert=True)
        return

    plan_code = callback.data.split(":", maxsplit=1)[1]
    plan = PLANS.get(plan_code)
    if not plan:
        await callback.answer("Неизвестный тариф", show_alert=True)
        return

    upsert_user(callback.from_user.id, callback.from_user.username)

    order_id = create_payment_order(callback.from_user.id, plan)
    payment_url = build_donation_url(order_id, plan.amount_rub)

    text = (
        f"🧾 Заказ создан: {order_id}\n"
        f"Тариф: {plan.title}\n"
        f"Сумма: {plan.amount_rub} ₽\n\n"
        "Как оплатить:\n"
        "1. Нажмите «Оплатить в DonationAlerts».\n"
        f"2. В комментарии к донату вставьте код: {order_id}\n"
        "3. После оплаты нажмите «Проверить оплату»."
    )

    await callback.message.answer(text, reply_markup=build_payment_keyboard(payment_url, order_id))
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

    if payment["status"] == "paid":
        user = get_user(callback.from_user.id)
        expiry = user["subscription_end"] if user else "-"
        await callback.answer("Оплата подтверждена", show_alert=False)
        await callback.message.answer(f"✅ Оплата принята. Подписка активна до: {expiry}")
        return

    await callback.answer("Пока не оплачено", show_alert=False)
    await callback.message.answer(
        "⏳ Платеж еще не подтвержден. Если вы уже оплатили, подождите 10-30 секунд и повторите проверку."
    )


@dp.message(F.text == "🚀 Подключить VPN")
async def vpn_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)
    user = get_user(message.from_user.id)

    if not user or not has_active_subscription(user["subscription_end"]):
        await message.answer(
            "❌ У вас нет активной подписки. Откройте «Купить подписку».",
            reply_markup=build_main_keyboard(),
        )
        return

    try:
        user_uuid = await ensure_vless_uuid(message.from_user.id, user["vless_uuid"])
    except Exception as exc:  # noqa: BLE001
        await message.answer(
            "⚠️ Подписка активна, но не удалось подготовить ключ.\n"
            f"Обратитесь в поддержку: {SUPPORT_CONTACT}\n"
            f"Техническая ошибка: {exc}"
        )
        return

    link = generate_vless_link(user_uuid)
    await message.answer(
        f"🔑 Ваш VLESS-ключ:\n{link}\n\n"
        "📚 Если нужно, откройте «Инструкции» для вашей ОС."
    )


@dp.message(F.text == "👤 Личный кабинет")
async def profile_handler(message: Message) -> None:
    upsert_user(message.from_user.id, message.from_user.username)
    user = get_user(message.from_user.id)

    if not user:
        await message.answer("Пользователь не найден. Нажмите /start")
        return

    status = "активна" if has_active_subscription(user["subscription_end"]) else "неактивна"
    subscription_end = user["subscription_end"] or "-"
    user_uuid = user["vless_uuid"] or "-"

    text = (
        "👤 Личный кабинет\n"
        f"Пользователь: {user_title(message)}\n"
        f"Подписка: {status}\n"
        f"До: {subscription_end}\n"
        f"UUID: {user_uuid}"
    )
    await message.answer(text)


@dp.message(F.text == "📚 Инструкции")
async def guides_handler(message: Message) -> None:
    await message.answer(
        "📚 Выберите вашу ОС/приложение:\n"
        "Основные клиенты: Happ и V2rayTun.",
        reply_markup=build_guides_keyboard(),
    )


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
    await message.answer(
        "🛟 Поддержка\n"
        f"Если не пришел ключ или есть вопросы по подключению, пишите: {SUPPORT_CONTACT}"
    )


@dp.message(F.text == "🎁 Тест на 3 дня")
async def trial_handler(message: Message) -> None:
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

    conn = get_conn()
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

    await message.answer(
        f"✅ Тест активирован на {TRIAL_DAYS} дня(дней).\n"
        "Нажмите «Подключить VPN» чтобы получить ключ."
    )


async def start_webhook_server() -> web.AppRunner:
    app = make_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_HOST, WEBHOOK_PORT)
    await site.start()

    path = DONATIONALERTS_WEBHOOK_PATH if DONATIONALERTS_WEBHOOK_PATH.startswith("/") else f"/{DONATIONALERTS_WEBHOOK_PATH}"
    print(f"[webhook] Listening on http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{path}")
    print(f"[webapp] Internal URL http://{WEBHOOK_HOST}:{WEBHOOK_PORT}/webapp")
    if WEBAPP_PUBLIC_URL:
        print(f"[webapp] Public URL {WEBAPP_PUBLIC_URL}")
    return runner


async def main() -> None:
    init_db()
    webhook_runner = await start_webhook_server()
    try:
        await dp.start_polling(bot)
    finally:
        await webhook_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
