# BoxVolt VPN Bot

Telegram-бот для продажи и управления VPN-подпиской (VLESS Reality через 3x-ui) с оплатой через DonatePay/CryptoBot и webhook/API-синхронизацией.

## Что уже реализовано

- Оплата через DonatePay и CryptoBot (без Telegram Payments / YooKassa).
- Создание заказа в боте с уникальным `order_id`.
- Проверка оплаты через webhook + защита секретом.
- Фоновая синхронизация платежей DonatePay через API.
- Автопродление `subscription_end` после успешной оплаты.
- Автосоздание VLESS-клиента в 3x-ui и отправка ключа в Telegram.
- Динамические цены из `pricing.json` (подхват без перезапуска бота).
- Скидки (глобальные и по тарифам) + админ-рассылка акции.
- Реферальная программа: deep-link + автозачисление бонусных дней за оплаченные приглашения.
- Антифрод: rate-limit заказов, burst-лимит, blacklist, защита trial по username/cooldown.
- Админ-метрики `/admin_stats` + ежедневный авто-отчет в админ-чат.
- Главное меню + под-кнопки (inline) для тарифов и инструкций.
- Telegram Mini App (`/webapp`) с оплатой и проверкой статуса.
- Встроенная админ-панель прямо в Mini App (для admin ID): цены/акции/рассылка.
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
- `DONATEPAY_DONATE_BASE_URL`
- `DONATEPAY_API_KEY`
- `DONATEPAY_WEBHOOK_SECRET`
- `CRYPTOBOT_ENABLED=1` и `CRYPTOBOT_API_TOKEN` (если хотите включить CryptoBot)
- `WEBAPP_PUBLIC_URL`
- `PRICING_FILE`
- `ADMIN_TELEGRAM_IDS` (для команды рассылки акции)
- `ADMIN_NOTIFY_CHAT_IDS` (опционально: chat_id групп/каналов для уведомлений об оплате)
- `ADMIN_NOTIFY_TOPIC_ID` (опционально: ID топика в группе для уведомлений об оплате)
- `REFERRAL_ENABLED`, `REFERRAL_REWARD_DAYS`, `REFERRAL_MIN_PLAN_DAYS` (реферальная программа)
- `BLACKLIST_TELEGRAM_IDS` (опционально: статический blacklist через `.env`)
- `ORDER_CREATE_COOLDOWN_SECONDS`, `ORDER_BURST_WINDOW_SECONDS`, `ORDER_BURST_MAX` (лимиты создания заказов)
- `TRIAL_REQUEST_COOLDOWN_SECONDS`, `TRIAL_USERNAME_UNIQUE` (защита trial)
- `ADMIN_DAILY_REPORT_ENABLED`, `ADMIN_DAILY_REPORT_INTERVAL_SECONDS` (ежедневный отчет)
- `PAYMENT_PENDING_TTL_MINUTES` (авто-отмена неоплаченного заказа)
- `PAYMENT_CLEANUP_INTERVAL_SECONDS` (интервал фоновой очистки)

## Динамические цены и скидки

Файл цен: `pricing.json`.

Пример:

```json
{
  "global_discount_percent": 0,
  "sale_title": "Весенняя акция",
  "sale_message": "Только до конца недели.",
  "plans": [
    {"code":"m1","title":"30 дней","days":30,"amount_rub":50,"discount_percent":0},
    {"code":"m3","title":"90 дней","days":90,"amount_rub":120,"discount_percent":0}
  ]
}
```

Как работает:
- Бот автоматически подхватывает изменения `pricing.json` без перезапуска.
- `global_discount_percent` применяется ко всем планам.
- `discount_percent` внутри плана добавляется к глобальной скидке.
- Максимальная скидка ограничена 90%.

Команды:
- `/prices` — показать актуальные тарифы и активную акцию.
- `/rules` — показать правила сервиса.
- `/myid` — показать ваш Telegram ID (удобно для `ADMIN_TELEGRAM_IDS`).
- `/admin` — открыть админ-панель (только для admin ID).
- `/admin_stats` — метрики (24ч / 7д / всего).
- `/sale_notify` — админ-рассылка текста акции из `pricing.json`.
- `/sale_notify ваш текст` — админ-рассылка произвольного текста.
- `/blacklist_add <tg_id> [причина]` — добавить в blacklist.
- `/blacklist_del <tg_id>` — удалить из blacklist.
- `/blacklist_list` — показать blacklist.

Mini App Admin:
- Если ваш ID есть в `ADMIN_TELEGRAM_IDS`, в Mini App появится блок `🛠 Админ-панель`.
- Доступно: редактирование планов, сохранение цен, обновление акции, рассылка пользователям.

Важно: для админ-команд укажите в `.env` список ID:
`ADMIN_TELEGRAM_IDS=123456789,987654321`

Для уведомлений об оплате в админ-чат(ы) можно указать:
`ADMIN_NOTIFY_CHAT_IDS=-1001234567890,-1009876543210`

Для отправки именно в топик (форум-тему) укажите:
`ADMIN_NOTIFY_TOPIC_ID=12345`
`Topic ID` можно получить командой `/myid`, отправив её внутри нужного топика.

## DonatePay: webhook + API sync

Если в вашем кабинете DonatePay есть вебхук, настройте URL:

```text
https://YOUR_DOMAIN/donatepay/webhook?secret=YOUR_SECRET
```

Где `YOUR_SECRET` должен совпадать с `DONATEPAY_WEBHOOK_SECRET` в `.env`.

Если вебхука в интерфейсе нет, это нормально: бот работает без него через API polling (`DONATEPAY_API_KEY`, `DONATEPAY_POLL_ENABLED=1`).

Важно:
- Бот связывает платеж с пользователем по `order_id`.
- Также поддерживается fallback через `metadata.telegram_id` (берется последний pending-заказ пользователя).
- В ссылке оплаты `order_id` подставляется в комментарий автоматически.
- После webhook с успешной оплатой подписка продлевается автоматически.
- Если webhook не пришел, фоновый poll подтянет платеж через API DonatePay.
- Неоплаченный заказ отменяется автоматически через `PAYMENT_PENDING_TTL_MINUTES` минут.

## CryptoBot (Crypto Pay API)

Включение в `.env`:

```env
CRYPTOBOT_ENABLED=1
CRYPTOBOT_API_TOKEN=YOUR_CRYPTO_PAY_API_TOKEN
CRYPTOBOT_WEBHOOK_PATH=/cryptobot/webhook
CRYPTOBOT_POLL_ENABLED=1
```

Webhook URL для CryptoBot:

```text
https://YOUR_DOMAIN/cryptobot/webhook
```

Рекомендуется:
- включать `CRYPTOBOT_VALIDATE_SIGNATURE=1`, если прокси не ломает исходные webhook-заголовки;
- при необходимости задать `CRYPTOBOT_WEBHOOK_SECRET` и передавать его в URL/заголовке;
- не отключать poll (`CRYPTOBOT_POLL_ENABLED=1`) как fallback, если webhook задерживается.

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

## Browser Extension API

Для браузерного расширения (Chrome/Edge/Opera) доступны роуты:

- `POST /edge/api/auth/start` — создать запрос входа (возвращает `bot_start_url` + `request_id/poll_token`).
- `POST /edge/api/auth/poll` — дождаться подтверждения через `/start edgeauth_<code>`.
- `GET /edge/api/me` — получить профиль, статус подписки и BR/RU proxy-конфиги.
- `POST /edge/api/logout` — завершить сессию расширения.

Новые переменные `.env` для расширения:

- `EDGE_EXTENSION_ENABLED`, `EDGE_AUTH_PREFIX`, `EDGE_AUTH_REQUEST_TTL_SECONDS`
- `EDGE_SESSION_TTL_SECONDS`, `EDGE_MAX_ACTIVE_SESSIONS_PER_USER`
- `EDGE_SERVER_BR_*`, `EDGE_SERVER_RU_*`

Шаблон MV3-расширения для Chrome/Edge/Opera лежит в папке `browser-extension/`.
Шаблон Firefox (AMO) лежит в папке `firefox-extension/`.

Безопасный прод-план (отдельные SOCKS5 порты, без изменений x-ui/443):
- `docs/EDGE_EXTENSION_PROD_SETUP.md`
- `scripts/install_edge_socks5.sh`
- `scripts/check_edge_socks5.sh`

## Домен и HTTPS

Для webhook нужен публичный HTTPS.
Если SSL на домене не активирован, webhook не будет стабильно работать.

## Ограничения платежей

Проверьте правила выбранного провайдера платежей перед запуском приема оплаты.

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

    location /donatepay/webhook {
        proxy_pass http://127.0.0.1:8080/donatepay/webhook;
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
- DonatePay webhook endpoint: `https://YOUR_DOMAIN/donatepay/webhook`.
- CryptoBot webhook endpoint: `https://YOUR_DOMAIN/cryptobot/webhook`.
- WebApp: откройте `https://YOUR_DOMAIN/webapp` (из Telegram).
- VPN: нажмите `🚀 Подключить VPN` и проверьте, что выдан валидный `vless://...` ключ.

## Безопасность

- Не храните секреты в `bot.py`.
- Не коммитьте `.env`.
- Регулярно меняйте `DONATEPAY_WEBHOOK_SECRET`.
