const STORAGE_KEYS = {
  SESSION_TOKEN: "edge_session_token",
  SELECTED_SERVER_ID: "edge_selected_server_id",
  PENDING_AUTH: "edge_pending_auth",
  THEME: "edge_theme"
};

const THEMES = {
  DARK: "dark",
  LIGHT: "light"
};

const STATUS_KIND = {
  DEFAULT: "",
  OK: "ok",
  ERR: "err"
};

const runtimeStatusEl = document.getElementById("runtimeStatus");
const powerHintEl = document.getElementById("powerHint");
const statusTextEl = document.getElementById("statusText");
const authCardEl = document.getElementById("authCard");
const controlCardEl = document.getElementById("controlCard");
const accountCardEl = document.getElementById("accountCard");
const loginBtn = document.getElementById("loginBtn");
const loginHelpEl = document.getElementById("loginHelp");
const refreshBtn = document.getElementById("refreshBtn");
const logoutBtn = document.getElementById("logoutBtn");
const manageBtn = document.getElementById("manageBtn");
const supportBtn = document.getElementById("supportBtn");
const botFooterBtn = document.getElementById("botFooterBtn");
const serverSelectWrapEl = document.getElementById("serverSelectWrap");
const serverPickerBtn = document.getElementById("serverPickerBtn");
const serverPickerFlagEl = document.getElementById("serverPickerFlag");
const serverPickerLabelEl = document.getElementById("serverPickerLabel");
const serverMenuEl = document.getElementById("serverMenu");
const powerBtn = document.getElementById("powerBtn");
const userNameEl = document.getElementById("userName");
const subscriptionStateEl = document.getElementById("subscriptionState");
const subscriptionEndEl = document.getElementById("subscriptionEnd");
const renewLinkEl = document.getElementById("renewLink");
const webappLinkEl = document.getElementById("webappLink");
const supportLinkEl = document.getElementById("supportLink");
const themeToggleBtn = document.getElementById("themeToggleBtn");

let sessionToken = "";
let meData = null;
let selectedServerId = "";
let pendingAuth = null;
let proxyState = {
  connected: false,
  serverId: null,
  serverTitle: null,
  healthWarning: null
};
let isAuthPolling = false;
let isPowerBusy = false;
let isServerMenuOpen = false;
let activeTheme = THEMES.DARK;

function configValue(key, fallback) {
  if (typeof BOXVOLT_EDGE_CONFIG !== "object" || BOXVOLT_EDGE_CONFIG === null) {
    return fallback;
  }
  return Object.prototype.hasOwnProperty.call(BOXVOLT_EDGE_CONFIG, key)
    ? BOXVOLT_EDGE_CONFIG[key]
    : fallback;
}

function apiBaseUrl() {
  return String(configValue("apiBaseUrl", "")).trim().replace(/\/+$/, "");
}

function demoModeEnabled() {
  return Boolean(configValue("demoMode", false));
}

function demoBypassSubscription() {
  return Boolean(configValue("demoBypassSubscription", false));
}

function normalizeTheme(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === THEMES.LIGHT) {
    return THEMES.LIGHT;
  }
  return THEMES.DARK;
}

function updateThemeButton() {
  if (!themeToggleBtn) {
    return;
  }
  if (activeTheme === THEMES.DARK) {
    themeToggleBtn.textContent = "☀";
    themeToggleBtn.title = "Светлая тема";
  } else {
    themeToggleBtn.textContent = "🌙";
    themeToggleBtn.title = "Темная тема";
  }
}

function applyTheme(theme) {
  activeTheme = normalizeTheme(theme);
  document.documentElement.setAttribute("data-theme", activeTheme);
  updateThemeButton();
}

function normalizeDemoServer(server, index) {
  const idRaw = String(server && server.id ? server.id : "").trim();
  const id = idRaw || `demo-${index + 1}`;
  const countryCode = String(server && server.country_code ? server.country_code : "").trim().toUpperCase() || "DEMO";
  const title = String(server && server.title ? server.title : "").trim() || `Demo ${countryCode}`;
  return {
    id,
    country_code: countryCode,
    title,
    demo: true,
    available: true
  };
}

function configuredDemoServers() {
  if (!demoModeEnabled()) {
    return [];
  }
  const raw = configValue("demoServers", []);
  if (!Array.isArray(raw) || !raw.length) {
    return [];
  }
  return raw.map((item, index) => normalizeDemoServer(item, index));
}

function hasUsableServerConfig(server) {
  if (!server) {
    return false;
  }
  if (server.demo === true) {
    return true;
  }
  return Boolean(server.host && server.port);
}

function subscriptionOrDemoAllowed() {
  if (subscriptionActive()) {
    return true;
  }
  return demoModeEnabled() && demoBypassSubscription();
}

function effectiveServers() {
  const apiServers = meData && Array.isArray(meData.servers) ? meData.servers : [];
  const apiHasUsable = apiServers.some((item) => hasUsableServerConfig(item));
  if (apiHasUsable) {
    return apiServers;
  }
  return configuredDemoServers();
}

function storageGet(keys) {
  return new Promise((resolve) => {
    chrome.storage.local.get(keys, (result) => {
      resolve(result || {});
    });
  });
}

function storageSet(values) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.set(values, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve();
    });
  });
}

function storageRemove(keys) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.remove(keys, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve();
    });
  });
}

function runtimeSend(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(message, (response) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve(response || {});
    });
  });
}

function openTab(url) {
  return new Promise((resolve, reject) => {
    chrome.tabs.create({ url }, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve();
    });
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTerminalAuthError(code) {
  const value = String(code || "").trim().toLowerCase();
  return [
    "request_expired",
    "bad_poll_token",
    "request_not_found",
    "missing_request_id",
    "missing_poll_token",
    "bad_request_id"
  ].includes(value);
}

function normalizePendingAuth(raw) {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const requestId = String(raw.request_id || "").trim();
  const pollToken = String(raw.poll_token || "").trim();
  if (!requestId || !pollToken) {
    return null;
  }
  return {
    request_id: requestId,
    poll_token: pollToken,
    expires_at: String(raw.expires_at || "").trim() || null,
    poll_interval_ms: Number(raw.poll_interval_ms || 0) || null,
    started_at: Number(raw.started_at || Date.now())
  };
}

async function setPendingAuth(raw) {
  const normalized = normalizePendingAuth(raw);
  pendingAuth = normalized;
  if (!normalized) {
    await storageRemove([STORAGE_KEYS.PENDING_AUTH]);
    return;
  }
  await storageSet({ [STORAGE_KEYS.PENDING_AUTH]: normalized });
}

function setServerMenuOpen(open) {
  isServerMenuOpen = Boolean(open) && Boolean(sessionToken) && effectiveServers().length > 0;
  serverMenuEl.classList.toggle("hidden", !isServerMenuOpen);
  serverPickerBtn.classList.toggle("open", isServerMenuOpen);
}

function setRuntimeStatus(text, kind = "") {
  runtimeStatusEl.textContent = text;
  runtimeStatusEl.className = "connection-state";
  if (kind === STATUS_KIND.OK) runtimeStatusEl.classList.add("ok");
  if (kind === STATUS_KIND.ERR) runtimeStatusEl.classList.add("err");
}

function setStatus(text, kind = "") {
  statusTextEl.textContent = text;
  statusTextEl.className = "status";
  if (kind === STATUS_KIND.OK) statusTextEl.classList.add("ok");
  if (kind === STATUS_KIND.ERR) statusTextEl.classList.add("err");
}

function toFriendlyError(error) {
  const code = String(error && error.code ? error.code : error && error.message ? error.message : "unknown_error");
  const mapping = {
    api_base_missing: "В config.js не указан apiBaseUrl.",
    edge_disabled: "Функция API расширения отключена на сервере.",
    network_error: "Нет связи с сервером.",
    invalid_json: "Сервер вернул неверный ответ.",
    request_expired: "Код входа истек. Запустите вход заново.",
    bad_poll_token: "Сессия входа недействительна. Повторите вход.",
    request_not_found: "Запрос входа не найден. Повторите вход.",
    missing_token: "Сессия не найдена. Войдите заново.",
    invalid_session: "Сессия невалидна. Войдите заново.",
    session_expired: "Сессия истекла. Войдите заново.",
    session_revoked: "Сессия завершена. Войдите заново.",
    auth_timeout: "Не получили подтверждение от Telegram вовремя.",
    auth_create_failed: "Не удалось создать запрос входа.",
    auth_failed: "Не удалось завершить авторизацию.",
    bad_server_config: "Конфиг сервера неполный.",
    proxy_control_unavailable: "Прокси в браузере управляется другим расширением или политикой. Отключите другое VPN/прокси.",
    external_vpn_conflict: "Обнаружен конфликт с VPN на ПК. Отключите VPN на компьютере и попробуйте снова.",
    proxy_healthcheck_failed: "Прокси не отвечает. Проверьте сервер и попробуйте снова.",
    proxy_connect_failed: "Не удалось включить прокси в браузере.",
    proxy_disconnect_failed: "Не удалось отключить прокси в браузере."
  };
  return mapping[code] || `Ошибка: ${code}`;
}

async function requestJson(path, options = {}, token = "") {
  const base = apiBaseUrl();
  if (!base) {
    throw { code: "api_base_missing" };
  }

  const headers = Object.assign({}, options.headers || {});
  if (!headers["Content-Type"] && options.body) {
    headers["Content-Type"] = "application/json";
  }
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 20000);
  let response;
  try {
    response = await fetch(`${base}${path}`, {
      method: options.method || "GET",
      headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: controller.signal
    });
  } catch (err) {
    clearTimeout(timeoutId);
    throw { code: "network_error", details: err };
  }
  clearTimeout(timeoutId);

  let data = null;
  try {
    data = await response.json();
  } catch (err) {
    throw { code: "invalid_json", details: err };
  }

  if (!response.ok || (data && data.ok === false)) {
    const errorCode = String((data && data.error) || `http_${response.status}`);
    throw { code: errorCode, details: data };
  }

  return data;
}

function selectedServerFromData() {
  const servers = effectiveServers();
  if (!servers.length) {
    return null;
  }

  let selected = servers.find((item) => String(item.id) === String(selectedServerId));
  if (!selected) {
    selected = servers[0];
    selectedServerId = String(selected.id || "");
  }
  return selected;
}

async function persistSelectedServer(serverId) {
  selectedServerId = String(serverId || "");
  await storageSet({ [STORAGE_KEYS.SELECTED_SERVER_ID]: selectedServerId });
}

function subscriptionActive() {
  return Boolean(meData && meData.subscription && meData.subscription.active);
}

function normalizeAbsoluteUrl(raw) {
  const value = String(raw || "").trim();
  if (!value) {
    return "";
  }
  if (!/^https?:\/\//i.test(value)) {
    return "";
  }
  return value;
}

function normalizeSupportUrl(raw) {
  const value = String(raw || "").trim();
  if (!value) {
    return "";
  }
  if (/^https?:\/\//i.test(value)) {
    return value;
  }
  if (/^t\.me\//i.test(value)) {
    return `https://${value}`;
  }
  if (value.startsWith("@")) {
    const username = value.slice(1).trim();
    if (!username) {
      return "";
    }
    return `https://t.me/${username}`;
  }
  if (/^[a-zA-Z0-9_]{3,64}$/.test(value)) {
    return `https://t.me/${value}`;
  }
  return "";
}

function resolveSupportTarget() {
  const apiContact = meData && meData.support_contact ? String(meData.support_contact) : "";
  const configContact = String(
    configValue("supportContact", configValue("supportUrl", ""))
  );
  return normalizeSupportUrl(apiContact) || normalizeSupportUrl(configContact);
}

function resolveBotTarget() {
  const botUrl = String(renewLinkEl.dataset.targetUrl || "").trim();
  const webappUrl = String(webappLinkEl.dataset.targetUrl || "").trim();
  const configBotUrl = normalizeAbsoluteUrl(configValue("botUrl", ""));
  return botUrl || configBotUrl || webappUrl;
}

function renderLinks() {
  const apiBotStartUrl = meData && meData.bot_start_url ? String(meData.bot_start_url) : "";
  const apiWebappUrl = meData && meData.webapp_url ? String(meData.webapp_url) : "";
  const configuredBotUrl = normalizeAbsoluteUrl(configValue("botUrl", ""));
  const configuredWebappUrl = normalizeAbsoluteUrl(configValue("webappUrl", ""));
  const supportUrl = resolveSupportTarget();
  const botStartUrl = apiBotStartUrl || configuredBotUrl || "#";
  const webappUrl = apiWebappUrl || configuredWebappUrl || "#";

  renewLinkEl.href = botStartUrl;
  webappLinkEl.href = webappUrl;
  renewLinkEl.dataset.targetUrl = botStartUrl && botStartUrl !== "#" ? botStartUrl : "";
  webappLinkEl.dataset.targetUrl = webappUrl && webappUrl !== "#" ? webappUrl : "";
  supportLinkEl.href = supportUrl || "#";
  supportLinkEl.dataset.targetUrl = supportUrl || "";
}

function extractFlag(label) {
  const text = String(label || "");
  const match = text.match(/[\u{1F1E6}-\u{1F1FF}]{2}/u);
  return match ? match[0] : "";
}

function stripLeadingFlag(label) {
  const text = String(label || "").trim();
  const stripped = text.replace(/^[\u{1F1E6}-\u{1F1FF}]{2}\s*/u, "").trim();
  return stripped || text;
}

function countryFlag(code) {
  const map = {
    RU: "🇷🇺",
    DE: "🇩🇪",
    BR: "🇧🇷",
    FR: "🇫🇷",
    KZ: "🇰🇿",
    AM: "🇦🇲"
  };
  return map[String(code || "").toUpperCase()] || "🌐";
}

function serverFlag(server) {
  const fromTitle = extractFlag(server && server.title ? server.title : "");
  if (fromTitle) {
    return fromTitle;
  }
  return countryFlag(server && server.country_code ? server.country_code : "");
}

function serverLabel(server) {
  if (!server) {
    return "Сервер";
  }
  const base = String(server.title || server.country_code || server.id || "Сервер");
  return stripLeadingFlag(base);
}

function renderServerPicker() {
  const selected = selectedServerFromData();
  const servers = effectiveServers();

  if (!selected) {
    serverPickerFlagEl.textContent = servers.length ? "🌐" : "⚠️";
    serverPickerLabelEl.textContent = servers.length ? "Выберите сервер" : "Серверы не настроены";
    serverPickerBtn.disabled = !sessionToken || !servers.length;
    return;
  }

  serverPickerFlagEl.textContent = serverFlag(selected);
  serverPickerLabelEl.textContent = serverLabel(selected);
  serverPickerBtn.disabled = !sessionToken || !servers.length;
}

function renderServerMenu() {
  serverMenuEl.replaceChildren();
  const servers = effectiveServers();
  if (!servers.length) {
    const empty = document.createElement("div");
    empty.className = "server-empty";
    empty.textContent = "Серверы не настроены.";
    serverMenuEl.appendChild(empty);
    return;
  }

  for (const server of servers) {
    const id = String(server.id || "");
    const available = Boolean(server.available) && subscriptionOrDemoAllowed();
    const option = document.createElement("button");
    option.type = "button";
    option.className = `server-option${id === selectedServerId ? " active" : ""}${available ? "" : " unavailable"}`;

    const subtitle = server.demo === true
      ? "demo-mode"
      : available
        ? "доступен"
        : "нет подписки";

    const left = document.createElement("span");
    left.className = "server-option-left";

    const flag = document.createElement("span");
    flag.className = "server-option-flag";
    flag.textContent = serverFlag(server);

    const textWrap = document.createElement("span");
    textWrap.className = "server-option-text";

    const title = document.createElement("span");
    title.className = "server-option-title";
    title.textContent = serverLabel(server);

    const sub = document.createElement("span");
    sub.className = "server-option-sub";
    sub.textContent = subtitle;

    textWrap.appendChild(title);
    textWrap.appendChild(sub);
    left.appendChild(flag);
    left.appendChild(textWrap);

    const check = document.createElement("span");
    check.className = "server-option-check";
    check.textContent = id === selectedServerId ? "✓" : "";

    option.appendChild(left);
    option.appendChild(check);

    option.addEventListener("click", async () => {
      await persistSelectedServer(id);
      setServerMenuOpen(false);
      render();
    });

    serverMenuEl.appendChild(option);
  }
}

function renderAccount() {
  const user = meData && meData.user ? meData.user : {};
  const subscription = meData && meData.subscription ? meData.subscription : {};

  userNameEl.textContent = user.username ? `@${user.username}` : `ID ${user.id || "-"}`;
  subscriptionStateEl.textContent = subscription.active ? "Активна" : "Не активна";
  subscriptionEndEl.textContent = subscription.subscription_end || "-";
}

function renderPowerState() {
  const selected = selectedServerFromData();
  const allowed = subscriptionOrDemoAllowed();
  const canConnect = Boolean(
    selected &&
    allowed &&
    (selected.demo === true || (selected.host && selected.port))
  );

  powerBtn.disabled = isPowerBusy || (!proxyState.connected && (!sessionToken || !canConnect));
  powerBtn.classList.toggle("is-on", proxyState.connected);
  powerBtn.classList.toggle("is-busy", isPowerBusy);

  if (proxyState.connected) {
    setRuntimeStatus("Вы подключены", STATUS_KIND.OK);
    powerHintEl.textContent = "Нажмите, чтобы отключиться";
  } else if (!sessionToken) {
    setRuntimeStatus("Ожидает вход", STATUS_KIND.DEFAULT);
    powerHintEl.textContent = "Сначала войдите через Telegram";
  } else if (!selected) {
    setRuntimeStatus("Выберите сервер", STATUS_KIND.ERR);
    powerHintEl.textContent = "Выберите сервер и нажмите кнопку";
  } else if (!allowed) {
    setRuntimeStatus("Подписка не активна", STATUS_KIND.ERR);
    powerHintEl.textContent = "Продлите подписку, затем подключайтесь";
  } else {
    setRuntimeStatus("Не подключено", STATUS_KIND.DEFAULT);
    powerHintEl.textContent = "Нажмите, чтобы подключиться";
  }

  if (!sessionToken) {
    manageBtn.disabled = true;
    manageBtn.textContent = "Открыть бота";
  } else if (subscriptionActive()) {
    manageBtn.disabled = false;
    manageBtn.textContent = "Открыть бота";
  } else {
    manageBtn.disabled = false;
    manageBtn.textContent = "Продлить подписку";
  }
}

function renderFooterActions() {
  const botTarget = resolveBotTarget();
  const supportTarget = String(supportLinkEl.dataset.targetUrl || "").trim();

  botFooterBtn.disabled = !botTarget;
  supportBtn.disabled = !supportTarget;
}

function render() {
  const loggedIn = Boolean(sessionToken);
  renderLinks();
  renderFooterActions();

  authCardEl.classList.toggle("hidden", loggedIn);
  controlCardEl.classList.toggle("hidden", !loggedIn);
  accountCardEl.classList.toggle("hidden", !loggedIn);

  if (loggedIn) {
    renderAccount();
    renderServerPicker();
    renderServerMenu();
  } else {
    setServerMenuOpen(false);
  }

  renderPowerState();
}

async function refreshProxyState() {
  let lastError = null;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const res = await runtimeSend({ type: "proxy-state" });
      if (!res || !res.ok || !res.state) {
        throw new Error("bad_proxy_state_response");
      }
      proxyState = {
        connected: Boolean(res.state.connected),
        serverId: res.state.serverId || null,
        serverTitle: res.state.serverTitle || null,
        healthWarning: res.state.healthWarning || null
      };
      return true;
    } catch (err) {
      lastError = err;
      if (attempt === 0) {
        await sleep(180);
      }
    }
  }
  console.error("proxy-state failed", lastError);
  return false;
}

async function clearSession() {
  sessionToken = "";
  meData = null;
  await storageRemove([STORAGE_KEYS.SESSION_TOKEN]);
}

async function hydrateFromStorage() {
  const stored = await storageGet([
    STORAGE_KEYS.SESSION_TOKEN,
    STORAGE_KEYS.SELECTED_SERVER_ID,
    STORAGE_KEYS.PENDING_AUTH,
    STORAGE_KEYS.THEME
  ]);
  sessionToken = String(stored[STORAGE_KEYS.SESSION_TOKEN] || "").trim();
  selectedServerId = String(stored[STORAGE_KEYS.SELECTED_SERVER_ID] || "").trim();
  pendingAuth = normalizePendingAuth(stored[STORAGE_KEYS.PENDING_AUTH]);
  applyTheme(normalizeTheme(stored[STORAGE_KEYS.THEME] || THEMES.DARK));
}

async function loadMe() {
  if (!sessionToken) {
    return false;
  }

  try {
    const data = await requestJson("/edge/api/me", { method: "GET" }, sessionToken);
    meData = data;
    if (!selectedServerId) {
      const first = effectiveServers().length ? effectiveServers()[0] : null;
      if (first) {
        await persistSelectedServer(first.id);
      }
    }
    setStatus("Данные аккаунта обновлены.", STATUS_KIND.OK);
    return true;
  } catch (err) {
    const code = String(err && err.code ? err.code : "");
    if (["missing_token", "invalid_session", "session_expired", "session_revoked"].includes(code)) {
      await clearSession();
      setStatus("Сессия завершена. Войдите снова.", STATUS_KIND.ERR);
      return false;
    }
    throw err;
  }
}

async function pollAuthUntilApproved(startPayload) {
  const intervalMs = Math.max(800, Number(startPayload.poll_interval_ms || configValue("authPollIntervalMs", 2500)));
  const timeoutMs = Math.max(20000, Number(configValue("authPollTimeoutMs", 180000)));
  const startedAt = Number(startPayload.started_at || Date.now());

  while (Date.now() - startedAt < timeoutMs) {
    await sleep(intervalMs);

    let pollData;
    try {
      pollData = await requestJson("/edge/api/auth/poll", {
        method: "POST",
        body: {
          request_id: startPayload.request_id,
          poll_token: startPayload.poll_token
        }
      });
    } catch (err) {
      if (String(err && err.code ? err.code : "") === "network_error") {
        continue;
      }
      throw err;
    }

    const status = String(pollData.status || "pending").toLowerCase();
    if (status === "pending") {
      continue;
    }
    if (status !== "approved" || !pollData.session_token) {
      throw { code: status || "auth_failed", details: pollData };
    }

    sessionToken = String(pollData.session_token || "").trim();
    meData = pollData;
    await storageSet({ [STORAGE_KEYS.SESSION_TOKEN]: sessionToken });
    await setPendingAuth(null);

    if (!selectedServerId) {
      const first = effectiveServers().length ? effectiveServers()[0] : null;
      if (first) {
        await persistSelectedServer(first.id);
      }
    }
    return;
  }

  throw { code: "auth_timeout" };
}

async function startLogin() {
  if (isAuthPolling) {
    return;
  }

  isAuthPolling = true;
  loginBtn.disabled = true;
  loginHelpEl.classList.remove("hidden");
  loginHelpEl.textContent = "Создаю запрос входа...";
  setStatus("Ожидание подтверждения в Telegram...");

  try {
    const startPayloadRaw = await requestJson("/edge/api/auth/start", {
      method: "POST",
      body: { client: "browser-extension" }
    });
    const startPayload = {
      ...startPayloadRaw,
      started_at: Date.now()
    };
    await setPendingAuth(startPayload);

    const command = String(startPayload.bot_start_command || "").trim();
    const botStartUrl = String(startPayload.bot_start_url || "").trim();
    loginHelpEl.textContent = command
      ? `Откройте Telegram и подтвердите вход: ${command}`
      : "Откройте Telegram-бота и подтвердите вход.";

    if (botStartUrl) {
      await openTab(botStartUrl);
    }

    await pollAuthUntilApproved(startPayload);
    await refreshProxyState();
    setStatus("Авторизация завершена.", STATUS_KIND.OK);
    render();
  } catch (err) {
    const code = String(err && err.code ? err.code : "");
    if (isTerminalAuthError(code)) {
      await setPendingAuth(null);
    }
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  } finally {
    isAuthPolling = false;
    loginBtn.disabled = false;
  }
}

async function resumePendingAuth() {
  if (!pendingAuth || isAuthPolling || sessionToken) {
    return false;
  }

  isAuthPolling = true;
  loginBtn.disabled = true;
  loginHelpEl.classList.remove("hidden");
  loginHelpEl.textContent = "Найдена незавершенная авторизация. Подтверждаю вход...";
  setStatus("Проверяю подтверждение входа в Telegram...");

  try {
    await pollAuthUntilApproved(pendingAuth);
    await refreshProxyState();
    setStatus("Авторизация завершена.", STATUS_KIND.OK);
    render();
    return true;
  } catch (err) {
    const code = String(err && err.code ? err.code : "");
    if (isTerminalAuthError(code)) {
      await setPendingAuth(null);
    }
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
    return false;
  } finally {
    isAuthPolling = false;
    loginBtn.disabled = false;
  }
}

async function connectSelectedServer() {
  if (!sessionToken || !meData) {
    setStatus("Сначала выполните вход.", STATUS_KIND.ERR);
    return;
  }
  if (!subscriptionOrDemoAllowed()) {
    setStatus("Подписка не активна.", STATUS_KIND.ERR);
    return;
  }

  const server = selectedServerFromData();
  if (!server || (server.demo !== true && (!server.host || !server.port))) {
    setStatus("Сервер недоступен или не настроен.", STATUS_KIND.ERR);
    return;
  }

  isPowerBusy = true;
  render();
  try {
    const res = await runtimeSend({ type: "proxy-connect", server });
    if (!res || !res.ok) {
      throw { code: (res && res.error) || "proxy_connect_failed" };
    }
    await refreshProxyState();
    if (proxyState.healthWarning === "proxy_healthcheck_skipped") {
      setStatus(
        `Подключено: ${serverLabel(server)}. Проверка канала пропущена браузером, но прокси включен.`,
        STATUS_KIND.OK
      );
    } else {
      setStatus(`Подключено: ${serverLabel(server)}`, STATUS_KIND.OK);
    }
  } catch (err) {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  } finally {
    isPowerBusy = false;
    render();
  }
}

async function disconnectProxy() {
  isPowerBusy = true;
  render();
  try {
    const res = await runtimeSend({ type: "proxy-disconnect" });
    if (!res || !res.ok) {
      throw { code: (res && res.error) || "proxy_disconnect_failed" };
    }
    await refreshProxyState();
    setStatus("Прокси отключен.", STATUS_KIND.OK);
  } catch (err) {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  } finally {
    isPowerBusy = false;
    render();
  }
}

async function togglePower() {
  if (proxyState.connected) {
    await disconnectProxy();
  } else {
    await connectSelectedServer();
  }
}

async function doLogout() {
  try {
    if (sessionToken) {
      await requestJson("/edge/api/logout", { method: "POST" }, sessionToken);
    }
  } catch (_err) {
    // ignore server-side logout errors and clear local state anyway
  }

  try {
    await runtimeSend({ type: "proxy-disconnect" });
  } catch (_err) {
    // ignore
  }

  await clearSession();
  await setPendingAuth(null);
  await refreshProxyState();
  setServerMenuOpen(false);
  setStatus("Вы вышли из аккаунта.", STATUS_KIND.OK);
  render();
}

async function openManage() {
  const target = resolveBotTarget();
  if (!target) {
    setStatus("Ссылка бота пока недоступна.", STATUS_KIND.ERR);
    return;
  }
  try {
    await openTab(target);
  } catch (err) {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  }
}

async function openSupport() {
  const target = String(supportLinkEl.dataset.targetUrl || "").trim();
  if (!target) {
    setStatus("Ссылка поддержки пока недоступна.", STATUS_KIND.ERR);
    return;
  }
  try {
    await openTab(target);
  } catch (err) {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  }
}

async function toggleTheme() {
  const next = activeTheme === THEMES.DARK ? THEMES.LIGHT : THEMES.DARK;
  applyTheme(next);
  await storageSet({ [STORAGE_KEYS.THEME]: next });
}

async function bootstrap() {
  const base = apiBaseUrl();
  if (!base || !/^https:\/\//i.test(base)) {
    setStatus("Заполните apiBaseUrl в config.js (https://...).", STATUS_KIND.ERR);
    applyTheme(THEMES.DARK);
    render();
    return;
  }

  await hydrateFromStorage();
  await refreshProxyState();

  if (sessionToken) {
    if (pendingAuth) {
      await setPendingAuth(null);
    }
    try {
      await loadMe();
    } catch (err) {
      setStatus(toFriendlyError(err), STATUS_KIND.ERR);
    }
  } else if (pendingAuth) {
    await resumePendingAuth();
  } else {
    setStatus("Войдите через Telegram для доступа к серверам.");
  }

  if (demoModeEnabled()) {
    loginHelpEl.classList.remove("hidden");
    loginHelpEl.textContent = "Тестовый режим: подключение работает в DEMO без реального прокси.";
  }

  render();
}

loginBtn.addEventListener("click", () => {
  startLogin().catch((err) => {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  });
});

powerBtn.addEventListener("click", () => {
  togglePower().catch((err) => {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  });
});

refreshBtn.addEventListener("click", async () => {
  try {
    let resumed = false;
    await refreshProxyState();
    if (sessionToken) {
      await loadMe();
    } else if (pendingAuth) {
      resumed = await resumePendingAuth();
    }
    if (!resumed) {
      setStatus("Обновлено.", STATUS_KIND.OK);
    }
  } catch (err) {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  }
  render();
});

logoutBtn.addEventListener("click", () => {
  doLogout().catch((err) => {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  });
});

manageBtn.addEventListener("click", () => {
  openManage().catch((err) => {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  });
});

botFooterBtn.addEventListener("click", () => {
  openManage().catch((err) => {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  });
});

supportBtn.addEventListener("click", () => {
  openSupport().catch((err) => {
    setStatus(toFriendlyError(err), STATUS_KIND.ERR);
  });
});

themeToggleBtn.addEventListener("click", () => {
  toggleTheme().catch(() => {});
});

serverPickerBtn.addEventListener("click", (event) => {
  event.preventDefault();
  event.stopPropagation();
  if (serverPickerBtn.disabled) {
    return;
  }
  setServerMenuOpen(!isServerMenuOpen);
});

document.addEventListener("click", (event) => {
  if (!isServerMenuOpen) {
    return;
  }
  const target = event.target;
  if (serverSelectWrapEl && target instanceof Node && !serverSelectWrapEl.contains(target)) {
    setServerMenuOpen(false);
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && isServerMenuOpen) {
    setServerMenuOpen(false);
  }
});

window.addEventListener("blur", () => {
  if (isServerMenuOpen) {
    setServerMenuOpen(false);
  }
});

bootstrap().catch((err) => {
  setStatus(toFriendlyError(err), STATUS_KIND.ERR);
});
