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

function proxySet(value) {
  return new Promise((resolve, reject) => {
    chrome.proxy.settings.set({ value, scope: "regular" }, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve();
    });
  });
}

function proxySettingsGet() {
  return new Promise((resolve) => {
    chrome.proxy.settings.get({ incognito: false }, (details) => {
      resolve(details || {});
    });
  });
}

async function ensureProxyControllable() {
  const details = await proxySettingsGet();
  const level = String(details && details.levelOfControl ? details.levelOfControl : "").trim().toLowerCase();
  if (level === "controlled_by_other_extensions" || level === "not_controllable") {
    throw new Error("proxy_control_unavailable");
  }
}

function normalizeScheme(rawScheme) {
  const scheme = String(rawScheme || "").trim().toLowerCase();
  if (["http", "https", "socks4", "socks5"].includes(scheme)) {
    return scheme;
  }
  return "http";
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
  return {
    mode: "fixed_servers",
    rules: {
      singleProxy: {
        scheme: server.scheme,
        host: server.host,
        port: Number(server.port)
      },
      bypassList: ["<local>"]
    }
  };
}

function currentProxyMatchesServer(details, server) {
  const value = details && details.value ? details.value : null;
  if (!value || String(value.mode || "") !== "fixed_servers") {
    return false;
  }
  const rules = value.rules || {};
  const targetHost = String(server.host || "").trim().toLowerCase();
  const targetPort = Number(server.port || 0);
  const targetScheme = normalizeScheme(server.scheme);

  const candidates = [
    rules.singleProxy || null,
    rules.proxyForHttp || null,
    rules.proxyForHttps || null,
    rules.fallbackProxy || null
  ].filter(Boolean);

  for (const candidate of candidates) {
    const currentHost = String(candidate.host || "").trim().toLowerCase();
    const currentPort = Number(candidate.port || 0);
    const currentScheme = normalizeScheme(candidate.scheme);
    if (
      currentHost === targetHost
      && currentPort === targetPort
      && currentScheme === targetScheme
    ) {
      return true;
    }
  }
  return false;
}

function proxySettingsMode(details) {
  const value = details && details.value ? details.value : null;
  return String(value && value.mode ? value.mode : "").trim().toLowerCase();
}

function extractActiveProxyFromDetails(details) {
  const value = details && details.value ? details.value : null;
  if (!value || proxySettingsMode(details) !== "fixed_servers") {
    return null;
  }
  const rules = value.rules || {};
  const candidate = (
    rules.singleProxy
    || rules.proxyForHttps
    || rules.proxyForHttp
    || rules.fallbackProxy
    || null
  );
  if (!candidate) {
    return null;
  }
  const host = String(candidate.host || "").trim();
  const port = Number(candidate.port || 0);
  if (!host || !Number.isFinite(port) || port <= 0) {
    return null;
  }
  return {
    host,
    port,
    scheme: normalizeScheme(candidate.scheme),
    title: `${host}:${port}`
  };
}

function proxyStateChanged(a, b) {
  return JSON.stringify(a || {}) !== JSON.stringify(b || {});
}

async function syncProxyStateFromBrowser() {
  const details = await proxySettingsGet();
  const mode = proxySettingsMode(details);

  // Demo profile uses direct mode but is treated as connected in UI.
  if (proxyState.connected && proxyState.serverConfig && proxyState.serverConfig.demo === true) {
    await setBadge(true);
    return proxyState;
  }

  let nextState = normalizeStoredProxyState(proxyState);
  if (mode === "fixed_servers") {
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

function setBadge(connected) {
  return new Promise((resolve) => {
    chrome.action.setBadgeText({ text: connected ? "ON" : "" }, () => {
      chrome.action.setBadgeBackgroundColor({ color: connected ? "#cc2d1f" : "#556070" }, () => {
        resolve();
      });
    });
  });
}

async function connectProxy(rawServer) {
  const server = asProxyServer(rawServer);
  if (server.demo) {
    await proxySet({ mode: "direct" });
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
      const fixedModeApplied = Boolean(value && String(value.mode || "") === "fixed_servers");
      if (currentProxyMatchesServer(details, server) || fixedModeApplied) {
        failedCode = "";
        healthWarning = "proxy_healthcheck_skipped";
      } else if (hadInternetBeforeProxy) {
        failedCode = "external_vpn_conflict";
      }
    }

    if (failedCode) {
      try {
        await proxySet({ mode: "direct" });
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
    healthWarning: healthWarning,
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
  await proxySet({ mode: "direct" });

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
        await proxySet({ mode: "direct" });
      } else {
        const server = asProxyServer(proxyState.serverConfig);
        await proxySet(proxyConfigFor(server));
      }
    } catch (err) {
      console.error("[edge-bg] Failed to restore proxy state", err);
      proxyState.connected = false;
      proxyState.serverConfig = null;
      proxyState.healthWarning = null;
      await storageSet({ [STORAGE_KEYS.STATE]: proxyState });
    }
  }

  await syncProxyStateFromBrowser();
}

function handleAuthRequired(details, callback) {
  if (!details || !details.isProxy) {
    callback({});
    return;
  }

  const localAuth = normalizeProxyAuth(proxyAuth);
  if (localAuth) {
    callback({ authCredentials: localAuth });
    return;
  }

  chrome.storage.local.get([STORAGE_KEYS.AUTH], (result) => {
    const storedAuth = normalizeProxyAuth(result && result[STORAGE_KEYS.AUTH]);
    if (storedAuth) {
      proxyAuth = storedAuth;
      callback({ authCredentials: storedAuth });
      return;
    }
    // Prevent browser popup with proxy host/IP when auth data is missing.
    callback({ cancel: true });
  });
}

chrome.webRequest.onAuthRequired.addListener(
  handleAuthRequired,
  { urls: ["<all_urls>"] },
  ["asyncBlocking"]
);

chrome.storage.onChanged.addListener((changes, areaName) => {
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
    console.error(`[edge-bg] ${logPrefix} restore failed`, err);
  });
  return restoreStatePromise;
}

chrome.runtime.onInstalled.addListener(() => {
  scheduleRestoreState("onInstalled");
});

chrome.runtime.onStartup.addListener(() => {
  scheduleRestoreState("onStartup");
});

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
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
