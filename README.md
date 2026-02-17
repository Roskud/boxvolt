# BoxVolt VPN Bot

Telegram-бот для продажи и управления VPN-подпиской (VLESS Reality через 3x-ui) с оплатой через DonationAlerts и webhook-подтверждением.

## Что уже реализовано

- Оплата через DonationAlerts (без Telegram Payments / YooKassa / DonatePay).
- Создание заказа в боте с уникальным `order_id`.
- Проверка оплаты через webhook + защита секретом.
- Автопродление `subscription_end` после успешной оплаты.
- Автосоздание VLESS-клиента в 3x-ui и отправка ключа в Telegram.
- Главное меню + под-кнопки (inline) для тарифов и инструкций.
- Telegram Mini App (`/webapp`) с оплатой и проверкой статуса.
- Инструкции для Android / iOS / Windows / macOS / Linux.

## Структура проекта

- `bot.py` — основной бот, webhook-сервер, логика оплаты, интеграция 3x-ui.
- `database.py` — инициализация/миграция схемы БД.
- `users.db` — SQLite база пользователей и платежей.
- `.env` — рабочие секреты и настройки.
- `.env.example` — шаблон переменных окружения.

## База данных

Таблица пользователей:

```sql
CREATE TABLE users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    subscription_end TEXT,
    vless_uuid TEXT,
    trial_used INTEGER DEFAULT 0
);
```

Таблица платежей:

```sql
CREATE TABLE payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT UNIQUE NOT NULL,
    telegram_id INTEGER NOT NULL,
    provider TEXT NOT NULL,
    amount_rub REAL NOT NULL,
    days INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    paid_at TEXT,
    raw_payload TEXT
);
```

## Настройка `.env`

1. Создайте рабочий конфиг из шаблона:

```bash
cp .env.example .env
```

2. Заполните минимум:

- `BOT_TOKEN`
- `XUI_URL`, `XUI_USERNAME`, `XUI_PASSWORD`, `INBOUND_ID`
- `SERVER_IP`, `PUBLIC_KEY`, `SHORT_ID`, `SNI`
- `DONATIONALERTS_USERNAME`
- `DONATIONALERTS_WEBHOOK_SECRET`
- `WEBAPP_PUBLIC_URL`

## DonationAlerts webhook

В `DonationAlerts` настройте webhook URL в формате:

```text
https://YOUR_DOMAIN/donationalerts/webhook?secret=YOUR_SECRET
```

Где `YOUR_SECRET` должен совпадать с `DONATIONALERTS_WEBHOOK_SECRET` в `.env`.

Важно:
- Бот связывает платеж с пользователем по `order_id`.
- Также поддерживается fallback через `metadata.telegram_id` (берется последний pending-заказ пользователя).
- Пользователь получает `order_id` в боте и вставляет его в комментарий к донату.
- После webhook с успешной оплатой подписка продлевается автоматически.

## Telegram WebApp

Mini App доступен по внутреннему пути `/webapp`.

В `.env`:

```env
WEBAPP_PUBLIC_URL=https://YOUR_DOMAIN/webapp
WEBAPP_INITDATA_MAX_AGE_SECONDS=86400
```

В BotFather для вашего бота задайте домен WebApp:

1. `/mybots` -> ваш бот -> `Bot Settings` -> `Menu Button`.
2. Выберите `Web App` и укажите `WEBAPP_PUBLIC_URL`.
3. (Опционально) добавьте через `/setdomain` этот же домен.

## Домен и HTTPS

Для webhook нужен публичный HTTPS.
Если SSL на домене не активирован, webhook не будет стабильно работать.

## Ограничения DonationAlerts

Проверьте правила DonationAlerts перед запуском платежей за VPN.
В официальной базе знаний указано, что сервис может ограничивать использование для продажи товаров/услуг и других сценариев, которые не соответствуют их политике.

## Запуск

### Linux (Ubuntu/Debian)

```bash
cd /root/boxvolt
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 bot.py
```

### macOS

```bash
cd /path/to/boxvolt
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 bot.py
```

### Windows (PowerShell)

```powershell
cd C:\path\to\boxvolt
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
py bot.py
```

## Запуск как systemd service (Linux)

```ini
[Unit]
Description=BoxVolt VPN Bot
After=network.target

[Service]
WorkingDirectory=/root/boxvolt
ExecStart=/root/boxvolt/.venv/bin/python /root/boxvolt/bot.py
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

## Reverse proxy пример (Nginx)

```nginx
server {
    listen 443 ssl;
    server_name your-domain.tld;

    ssl_certificate /etc/letsencrypt/live/your-domain.tld/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.tld/privkey.pem;

    location /donationalerts/webhook {
        proxy_pass http://127.0.0.1:8080/donationalerts/webhook;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /health {
        proxy_pass http://127.0.0.1:8080/health;
    }

    location /webapp {
        proxy_pass http://127.0.0.1:8080/webapp;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /webapp/api/ {
        proxy_pass http://127.0.0.1:8080/webapp/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## Инструкции для клиентов VPN

В боте есть раздел `📚 Инструкции` с профилями:

- Android: Happ / V2rayTun
- iOS: Happ
- Windows: V2rayTun
- macOS: Happ
- Linux: V2rayTun

Основной сценарий:
1. Купить подписку.
2. Получить VLESS ссылку.
3. Импортировать ссылку в Happ или V2rayTun.
4. Нажать Connect/Start.

## Проверка работоспособности

- Бот: отправьте `/start`.
- Webhook: откройте `https://YOUR_DOMAIN/health`.
- WebApp: откройте `https://YOUR_DOMAIN/webapp` (из Telegram).
- VPN: нажмите `🚀 Подключить VPN` и проверьте, что выдан валидный `vless://...` ключ.

## Безопасность

- Не храните секреты в `bot.py`.
- Не коммитьте `.env`.
- Регулярно меняйте `DONATIONALERTS_WEBHOOK_SECRET`.
