const STORAGE_KEYS = {
  STATE: "proxy_state",
  AUTH: "proxy_auth"
};

function emptyProxyState() {
  return {
    connected: false,
    serverId: null,
    serverTitle: null,
    serverConfig: null,
    healthWarning: null,
    updatedAt: 0
  };
}

function normalizeStoredProxyState(rawState) {
  if (!rawState || typeof rawState !== "object") {
    return emptyProxyState();
  }
  return {
    connected: Boolean(rawState.connected),
    serverId: rawState.serverId || null,
    serverTitle: rawState.serverTitle || null,
    serverConfig: rawState.serverConfig || null,
    healthWarning: rawState.healthWarning || null,
    updatedAt: Number(rawState.updatedAt || 0)
  };
}

let proxyState = emptyProxyState();
let proxyAuth = null;
let restoreStatePromise = Promise.resolve();

function normalizeProxyAuth(rawAuth) {
  if (!rawAuth || typeof rawAuth !== "object") {
    return null;
  }
  const username = String(rawAuth.username || "").trim();
  const password = String(rawAuth.password || "").trim();
  if (!username || !password) {
    return null;
  }
  return { username, password };
}

function normalizeScheme(rawScheme) {
  const scheme = String(rawScheme || "").trim().toLowerCase();
  if (["http", "https", "socks4", "socks5"].includes(scheme)) {
    return scheme;
  }
  return "http";
}

function schemeGroup(rawScheme) {
  const scheme = normalizeScheme(rawScheme);
  if (scheme === "socks4" || scheme === "socks5") {
    return "socks";
  }
  return "http";
}

function endpointFromHostPort(host, port) {
  return `${String(host || "").trim()}:${Number(port || 0)}`;
}

function parseHostPort(rawEndpoint) {
  const source = String(rawEndpoint || "").trim();
  if (!source) {
    return null;
  }

  const noScheme = source.replace(/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//, "");
  const value = noScheme.split("/")[0].trim();
  if (!value) {
    return null;
  }

  if (value.startsWith("[") && value.includes("]")) {
    const closeIndex = value.indexOf("]");
    const host = value.slice(1, closeIndex).trim();
    const after = value.slice(closeIndex + 1).trim();
    const portText = after.startsWith(":") ? after.slice(1).trim() : "";
    const port = Number(portText || 0);
    if (!host || !Number.isFinite(port) || port <= 0) {
      return null;
    }
    return { host, port };
  }

  const lastColon = value.lastIndexOf(":");
  if (lastColon <= 0 || value.indexOf(":") !== lastColon) {
    return null;
  }

  const host = value.slice(0, lastColon).trim();
  const port = Number(value.slice(lastColon + 1).trim());
  if (!host || !Number.isFinite(port) || port <= 0) {
    return null;
  }
  return { host, port };
}

async function storageGet(keys) {
  return browser.storage.local.get(keys);
}

async function storageSet(values) {
  await browser.storage.local.set(values);
}

async function proxySet(value) {
  await browser.proxy.settings.set({ value, scope: "regular" });
}

async function proxySettingsGet() {
  return browser.proxy.settings.get({ incognito: false });
}

async function ensureProxyControllable() {
  const details = await proxySettingsGet();
  const level = String(details && details.levelOfControl ? details.levelOfControl : "").trim().toLowerCase();
  if (level === "controlled_by_other_extensions" || level === "not_controllable") {
    throw new Error("proxy_control_unavailable");
  }
}

function asProxyServer(rawServer) {
  const isDemo = Boolean(rawServer && rawServer.demo === true);
  if (isDemo) {
    return {
      id: String(rawServer && rawServer.id ? rawServer.id : "").trim() || "demo",
      title: String(rawServer && rawServer.title ? rawServer.title : "").trim() || "Demo",
      demo: true,
      host: null,
      port: 0,
      scheme: "http",
      username: null,
      password: null
    };
  }

  const host = String(rawServer && rawServer.host ? rawServer.host : "").trim();
  const port = Number(rawServer && rawServer.port ? rawServer.port : 0);
  if (!host || !Number.isFinite(port) || port <= 0) {
    throw new Error("bad_server_config");
  }

  return {
    id: String(rawServer && rawServer.id ? rawServer.id : "").trim() || null,
    title: String(rawServer && rawServer.title ? rawServer.title : "").trim() || host,
    demo: false,
    host,
    port,
    scheme: normalizeScheme(rawServer && rawServer.scheme),
    username: String(rawServer && rawServer.username ? rawServer.username : "").trim() || null,
    password: String(rawServer && rawServer.password ? rawServer.password : "").trim() || null
  };
}

function proxyConfigFor(server) {
  const scheme = normalizeScheme(server.scheme);
  const endpoint = endpointFromHostPort(server.host, server.port);

  if (scheme === "socks4" || scheme === "socks5") {
    return {
      proxyType: "manual",
      socks: endpoint,
      socksVersion: scheme === "socks4" ? 4 : 5,
      proxyDNS: true,
      passthrough: "<local>"
    };
  }

  return {
    proxyType: "manual",
    http: endpoint,
    ssl: endpoint,
    passthrough: "<local>"
  };
}

function proxySettingsMode(details) {
  const value = details && details.value ? details.value : null;
  const proxyType = value && value.proxyType ? value.proxyType : "";
  return String(proxyType).trim().toLowerCase();
}

function extractActiveProxyFromDetails(details) {
  const value = details && details.value ? details.value : null;
  if (!value || proxySettingsMode(details) !== "manual") {
    return null;
  }

  if (value.socks) {
    const parsed = parseHostPort(value.socks);
    if (!parsed) {
      return null;
    }
    const socksVersion = Number(value.socksVersion || 5);
    const scheme = socksVersion === 4 ? "socks4" : "socks5";
    return {
      host: parsed.host,
      port: parsed.port,
      scheme,
      title: `${parsed.host}:${parsed.port}`
    };
  }

  const parsed = parseHostPort(value.http || value.ssl || "");
  if (!parsed) {
    return null;
  }
  return {
    host: parsed.host,
    port: parsed.port,
    scheme: "http",
    title: `${parsed.host}:${parsed.port}`
  };
}

function currentProxyMatchesServer(details, server) {
  const active = extractActiveProxyFromDetails(details);
  if (!active) {
    return false;
  }

  const activeHost = String(active.host || "").trim().toLowerCase();
  const targetHost = String(server.host || "").trim().toLowerCase();
  const activePort = Number(active.port || 0);
  const targetPort = Number(server.port || 0);

  return (
    activeHost === targetHost
    && activePort === targetPort
    && schemeGroup(active.scheme) === schemeGroup(server.scheme)
  );
}

function proxyStateChanged(a, b) {
  return JSON.stringify(a || {}) !== JSON.stringify(b || {});
}

async function syncProxyStateFromBrowser() {
  const details = await proxySettingsGet();
  const mode = proxySettingsMode(details);

  // Demo profile uses no proxy but should remain connected in UI.
  if (proxyState.connected && proxyState.serverConfig && proxyState.serverConfig.demo === true) {
    await setBadge(true);
    return proxyState;
  }

  let nextState = normalizeStoredProxyState(proxyState);
  if (mode === "manual") {
    const activeProxy = extractActiveProxyFromDetails(details);
    if (!activeProxy) {
      nextState = emptyProxyState();
      nextState.updatedAt = Date.now();
    } else {
      const knownConfig = (
        nextState.serverConfig
        && typeof nextState.serverConfig === "object"
        && nextState.serverConfig.demo !== true
      )
        ? nextState.serverConfig
        : null;
      const matchedKnownConfig = knownConfig ? currentProxyMatchesServer(details, knownConfig) : false;
      const serverConfig = matchedKnownConfig
        ? knownConfig
        : {
            id: nextState.serverId || null,
            title: nextState.serverTitle || activeProxy.title,
            demo: false,
            host: activeProxy.host,
            port: activeProxy.port,
            scheme: activeProxy.scheme
          };
      nextState = {
        connected: true,
        serverId: nextState.serverId || serverConfig.id || null,
        serverTitle: nextState.serverTitle || serverConfig.title || activeProxy.title,
        serverConfig,
        healthWarning: nextState.healthWarning || null,
        updatedAt: Date.now()
      };
    }
  } else {
    nextState = emptyProxyState();
    nextState.updatedAt = Date.now();
  }

  if (proxyStateChanged(proxyState, nextState)) {
    proxyState = nextState;
    await storageSet({ [STORAGE_KEYS.STATE]: proxyState });
  } else {
    proxyState = nextState;
  }

  await setBadge(Boolean(proxyState.connected));
  return proxyState;
}

async function fetchReachability(url, timeoutMs) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      cache: "no-store",
      mode: "no-cors",
      signal: controller.signal
    });
    return Boolean(res);
  } catch (_err) {
    return false;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function proxyHealthcheck() {
  const targets = [
    "https://connect.boxvolt.shop/status.json",
    "https://example.com/",
    "https://www.cloudflare.com/cdn-cgi/trace"
  ];
  for (const target of targets) {
    const ok = await fetchReachability(target, 12000);
    if (ok) {
      return;
    }
  }
  throw new Error("proxy_healthcheck_failed");
}

async function directHealthcheck() {
  const targets = [
    "https://connect.boxvolt.shop/status.json",
    "https://example.com/",
    "https://www.cloudflare.com/cdn-cgi/trace"
  ];
  for (const target of targets) {
    const ok = await fetchReachability(target, 7000);
    if (ok) {
      return true;
    }
  }
  return false;
}

async function setBadge(connected) {
  await browser.action.setBadgeText({ text: connected ? "ON" : "" });
  await browser.action.setBadgeBackgroundColor({ color: connected ? "#cc2d1f" : "#556070" });
}

async function connectProxy(rawServer) {
  const server = asProxyServer(rawServer);
  if (server.demo) {
    await proxySet({ proxyType: "none" });
    proxyAuth = null;
    proxyState = {
      connected: true,
      serverId: server.id,
      serverTitle: server.title,
      serverConfig: {
        id: server.id,
        title: server.title,
        demo: true
      },
      healthWarning: null,
      updatedAt: Date.now()
    };
    await storageSet({
      [STORAGE_KEYS.STATE]: proxyState,
      [STORAGE_KEYS.AUTH]: null
    });
    await setBadge(true);
    return proxyState;
  }

  const nextAuth = normalizeProxyAuth({
    username: server.username,
    password: server.password
  });
  proxyAuth = nextAuth;
  const hadInternetBeforeProxy = await directHealthcheck();

  let failedCode = "";
  let healthWarning = null;
  try {
    await ensureProxyControllable();
    await proxySet(proxyConfigFor(server));
    await proxyHealthcheck();
  } catch (err) {
    failedCode = err && err.message ? String(err.message) : "proxy_connect_failed";
    if (failedCode === "proxy_healthcheck_failed") {
      const details = await proxySettingsGet();
      const value = details && details.value ? details.value : null;
      const manualModeApplied = Boolean(
        value && String(value.proxyType || "").trim().toLowerCase() === "manual"
      );
      if (currentProxyMatchesServer(details, server) || manualModeApplied) {
        failedCode = "";
        healthWarning = "proxy_healthcheck_skipped";
      } else if (hadInternetBeforeProxy) {
        failedCode = "external_vpn_conflict";
      }
    }

    if (failedCode) {
      try {
        await proxySet({ proxyType: "none" });
        proxyAuth = null;
        proxyState = emptyProxyState();
        proxyState.updatedAt = Date.now();
        await storageSet({
          [STORAGE_KEYS.STATE]: proxyState,
          [STORAGE_KEYS.AUTH]: null
        });
        await setBadge(false);
      } catch (_rollbackErr) {
        // Ignore rollback errors; primary error should be propagated.
      }
      throw new Error(failedCode);
    }
  }

  proxyState = {
    connected: true,
    serverId: server.id,
    serverTitle: server.title,
    serverConfig: {
      id: server.id,
      title: server.title,
      demo: false,
      host: server.host,
      port: server.port,
      scheme: server.scheme
    },
    healthWarning,
    updatedAt: Date.now()
  };

  await storageSet({
    [STORAGE_KEYS.STATE]: proxyState,
    [STORAGE_KEYS.AUTH]: proxyAuth
  });
  await setBadge(true);
  return proxyState;
}

async function disconnectProxy() {
  await proxySet({ proxyType: "none" });

  proxyAuth = null;
  proxyState = emptyProxyState();
  proxyState.updatedAt = Date.now();

  await storageSet({
    [STORAGE_KEYS.STATE]: proxyState,
    [STORAGE_KEYS.AUTH]: null
  });
  await setBadge(false);
  return proxyState;
}

async function restoreState() {
  const stored = await storageGet([STORAGE_KEYS.STATE, STORAGE_KEYS.AUTH]);
  proxyState = normalizeStoredProxyState(stored[STORAGE_KEYS.STATE]);
  proxyAuth = normalizeProxyAuth(stored[STORAGE_KEYS.AUTH]);

  if (proxyState.connected && proxyState.serverConfig) {
    try {
      if (proxyState.serverConfig.demo === true) {
        await proxySet({ proxyType: "none" });
      } else {
        const server = asProxyServer(proxyState.serverConfig);
        await proxySet(proxyConfigFor(server));
      }
    } catch (err) {
      console.error("[firefox-bg] Failed to restore proxy state", err);
      proxyState.connected = false;
      proxyState.serverConfig = null;
      proxyState.healthWarning = null;
      await storageSet({ [STORAGE_KEYS.STATE]: proxyState });
    }
  }

  await syncProxyStateFromBrowser();
}

async function handleAuthRequired(details) {
  if (!details || !details.isProxy) {
    return {};
  }

  const localAuth = normalizeProxyAuth(proxyAuth);
  if (localAuth) {
    return { authCredentials: localAuth };
  }

  const result = await browser.storage.local.get([STORAGE_KEYS.AUTH]);
  const storedAuth = normalizeProxyAuth(result && result[STORAGE_KEYS.AUTH]);
  if (storedAuth) {
    proxyAuth = storedAuth;
    return { authCredentials: storedAuth };
  }

  // Prevent browser popup with proxy host/IP when auth data is missing.
  return { cancel: true };
}

browser.webRequest.onAuthRequired.addListener(
  handleAuthRequired,
  { urls: ["<all_urls>"] },
  ["blocking"]
);

browser.storage.onChanged.addListener((changes, areaName) => {
  if (areaName !== "local") {
    return;
  }
  if (changes[STORAGE_KEYS.AUTH]) {
    proxyAuth = normalizeProxyAuth(changes[STORAGE_KEYS.AUTH].newValue);
  }
  if (changes[STORAGE_KEYS.STATE]) {
    proxyState = normalizeStoredProxyState(changes[STORAGE_KEYS.STATE].newValue);
    setBadge(Boolean(proxyState.connected)).catch(() => {});
  }
});

function scheduleRestoreState(logPrefix) {
  restoreStatePromise = restoreState().catch((err) => {
    console.error(`[firefox-bg] ${logPrefix} restore failed`, err);
  });
  return restoreStatePromise;
}

browser.runtime.onInstalled.addListener(() => {
  scheduleRestoreState("onInstalled");
});

browser.runtime.onStartup.addListener(() => {
  scheduleRestoreState("onStartup");
});

browser.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  (async () => {
    await restoreStatePromise;

    const type = String(message && message.type ? message.type : "").trim();
    if (!type) {
      sendResponse({ ok: false, error: "bad_message" });
      return;
    }

    if (type === "proxy-connect") {
      const state = await connectProxy(message.server || {});
      sendResponse({ ok: true, state });
      return;
    }

    if (type === "proxy-disconnect") {
      const state = await disconnectProxy();
      sendResponse({ ok: true, state });
      return;
    }

    if (type === "proxy-state") {
      const state = await syncProxyStateFromBrowser();
      sendResponse({ ok: true, state });
      return;
    }

    sendResponse({ ok: false, error: "unknown_message" });
  })().catch((err) => {
    sendResponse({
      ok: false,
      error: err && err.message ? String(err.message) : "runtime_error"
    });
  });

  return true;
});

scheduleRestoreState("initial");
