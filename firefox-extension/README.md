# BoxVolt Firefox Extension (MV3)

## Что это
Расширение для Mozilla Firefox с входом через Telegram-бота и подключением браузерного proxy (BR/RU) через `browser.proxy.settings`.

## Быстрый старт
1. В `config.js` укажите:
   - `apiBaseUrl`: базовый URL вашего backend (например `https://connect.boxvolt.shop`).
2. В `.env` backend заполните `EDGE_SERVER_BR_*` и `EDGE_SERVER_RU_*`.
3. Перезапустите бот (`python3 bot.py`).
4. Для Firefox: откройте `about:debugging#/runtime/this-firefox` -> `Load Temporary Add-on...` и выберите файл `manifest.json` из папки `firefox-extension`.

## Тестовый режим

В текущем шаблоне включен `demoMode: true` в `config.js`.

- Авторизация через Telegram и API идет в реальный backend.
- Подключение BR/RU в demo-режиме не переключает браузер на реальный proxy.
- Для боевого релиза установите `demoMode: false` и заполните `EDGE_SERVER_BR_*`, `EDGE_SERVER_RU_*` в `.env`.

Есть готовый пример прод-конфига: `config.prod.example.js`.

## Публикация
Перед публикацией в Firefox Add-ons (AMO):
- замените тестовые значения в `config.js` на прод-домен;
- ограничьте `host_permissions` в `manifest.json` только нужными доменами;
- подготовьте privacy policy и описание платной подписки;
- проверьте работу логина `Telegram -> /start edgeauth_* -> approved`.
