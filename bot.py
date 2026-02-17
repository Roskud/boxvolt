import sqlite3
import datetime
import asyncio
import uuid
import httpx

from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    PreCheckoutQuery
)
from aiogram.filters import Command

# ========= НАСТРОЙКИ =========

BOT_TOKEN = "8408106551:AAFg-oPomzmL5UDZjmbcJKzKPjsrGZpkTCo"
PAYMENT_PROVIDER_TOKEN = "

XUI_URL = "http://185.23.19.82:2053"
XUI_USERNAME = "Roskud"
XUI_PASSWORD = "tashevskiy2007"
INBOUND_ID = 1

PUBLIC_KEY = "MFOjkgYkaUKKT_oiAmzcWr69qy67b-preFpN5v17DSQ"
SHORT_ID = "704f6a83"
SNI = "www.cloudflare.com"

PRICE_RUB = 50
DAYS = 30

# ==============================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ===== БАЗА =====

def init_db():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        username TEXT,
        subscription_end TEXT,
        vless_uuid TEXT
    )
    """)
    conn.commit()
    conn.close()

init_db()

# ===== КЛАВИАТУРА =====

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🚀 Подключить VPN")],
        [KeyboardButton(text="💳 Купить подписку 30 дней")],
        [KeyboardButton(text="👤 Личный кабинет")]
    ],
    resize_keyboard=True
)

# ===== СОЗДАНИЕ VLESS =====

async def create_vless_user(telegram_id):
    async with httpx.AsyncClient() as client:

        login_data = {
            "username": XUI_USERNAME,
            "password": XUI_PASSWORD
        }

        r = await client.post(f"{XUI_URL}/login", data=login_data)
        cookies = r.cookies

        user_uuid = str(uuid.uuid4())

        settings = {
            "clients": [
                {
                    "id": user_uuid,
                    "flow": "",
                    "email": str(telegram_id),
                    "limitIp": 1,
                    "totalGB": 50,
                    "expiryTime": 0,
                    "enable": True
                }
            ]
        }

        data = {
            "id": INBOUND_ID,
            "settings": str(settings).replace("'", '"')
        }

        await client.post(
            f"{XUI_URL}/panel/api/inbounds/addClient",
            data=data,
            cookies=cookies
        )

        return user_uuid


def generate_vless_link(user_uuid):
    return (
        f"vless://{user_uuid}@185.23.19.82:443"
        f"?type=tcp&security=reality&sni={SNI}"
        f"&fp=chrome&pbk={PUBLIC_KEY}&sid={SHORT_ID}"
        f"#BoxVoltVPN"
    )

# ===== START =====

@dp.message(Command("start"))
async def start_handler(message: Message):
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)",
        (message.from_user.id, message.from_user.username)
    )

    conn.commit()
    conn.close()

    await message.answer("🔥 BoxVolt VPN", reply_markup=main_kb)

# ===== ПОКУПКА =====

@dp.message(lambda m: m.text == "💳 Купить подписку 30 дней")
async def buy_handler(message: Message):

    prices = [
        LabeledPrice(
            label="VPN 30 дней",
            amount=PRICE_RUB * 100  # в копейках
        )
    ]

    await bot.send_invoice(
        chat_id=message.chat.id,
        title="BoxVolt VPN",
        description="Подписка 30 дней",
        payload="vpn_subscription",
        provider_token=PAYMENT_PROVIDER_TOKEN,
        currency="RUB",
        prices=prices,
        start_parameter="vpn"
    )

# ===== PRECHECKOUT =====

@dp.pre_checkout_query()
async def pre_checkout(pre_checkout_q: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

# ===== УСПЕШНАЯ ОПЛАТА =====

@dp.message(lambda m: m.successful_payment is not None)
async def successful_payment_handler(message: Message):

    telegram_id = message.from_user.id

    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    end_date = datetime.datetime.now() + datetime.timedelta(days=DAYS)

    cursor.execute(
        "UPDATE users SET subscription_end=? WHERE telegram_id=?",
        (end_date.strftime("%Y-%m-%d %H:%M:%S"), telegram_id)
    )

    conn.commit()

    # создаём VLESS
    user_uuid = await create_vless_user(telegram_id)

    cursor.execute(
        "UPDATE users SET vless_uuid=? WHERE telegram_id=?",
        (user_uuid, telegram_id)
    )

    conn.commit()
    conn.close()

    link = generate_vless_link(user_uuid)

    await message.answer(
        "✅ Оплата прошла успешно!\n\n"
        "🔑 Ваш VPN ключ:\n\n"
        f"{link}",
        reply_markup=main_kb
    )

# ===== VPN =====

@dp.message(lambda m: m.text == "🚀 Подключить VPN")
async def vpn_handler(message: Message):

    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT subscription_end, vless_uuid FROM users WHERE telegram_id=?",
        (message.from_user.id,)
    )

    row = cursor.fetchone()
    conn.close()

    if not row or not row[0]:
        await message.answer("❌ У вас нет активной подписки.")
        return

    end_date = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")

    if datetime.datetime.now() > end_date:
        await message.answer("❌ Подписка истекла.")
        return

    link = generate_vless_link(row[1])

    await message.answer(f"🔑 Ваш VPN ключ:\n\n{link}")

# ===== КАБИНЕТ =====

@dp.message(lambda m: m.text == "👤 Личный кабинет")
async def cabinet_handler(message: Message):

    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT subscription_end FROM users WHERE telegram_id=?",
        (message.from_user.id,)
    )

    row = cursor.fetchone()
    conn.close()

    if not row or not row[0]:
        await message.answer("❌ Подписка отсутствует.")
        return

    await message.answer(f"📅 Подписка активна до:\n{row[0]}")

# ===== ЗАПУСК =====

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
