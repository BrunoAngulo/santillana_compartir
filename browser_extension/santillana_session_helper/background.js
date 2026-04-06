const SOURCE_CONFIG = {
  pegasus: {
    id: "pegasus",
    label: "Procesos Pegasus",
    mode: "localStorage",
    bootstrapUrl: "https://https.santillanacompartir.com/",
    matchPatterns: ["https://https.santillanacompartir.com/*"],
    storageKey: "pgs-jwt-token",
  },
  richmond: {
    id: "richmond",
    label: "Richmond Studio",
    mode: "localStorage",
    bootstrapUrl: "https://richmondstudio.global/",
    matchPatterns: ["https://richmondstudio.global/*"],
    storageKey: "accessToken",
  },
  loqueleo: {
    id: "loqueleo",
    label: "Loqueleo",
    mode: "cookies",
    bootstrapUrl: "https://loqueleodigital.com/",
    cookiesUrl: "https://loqueleodigital.com/",
    cookieDomain: "loqueleodigital.com",
    sessionCookieName: "_session_id",
  },
};

function normalizeError(error) {
  if (!error) {
    return "Error desconocido";
  }
  if (typeof error === "string") {
    return error;
  }
  if (typeof error.message === "string" && error.message.trim()) {
    return error.message.trim();
  }
  return String(error);
}

function serializeTab(tab) {
  if (!tab || typeof tab !== "object") {
    return null;
  }
  return {
    id: typeof tab.id === "number" ? tab.id : null,
    title: String(tab.title || ""),
    url: String(tab.url || ""),
    status: String(tab.status || ""),
  };
}

async function queryExistingTab(source) {
  const tabs = await chrome.tabs.query({ url: source.matchPatterns });
  if (!Array.isArray(tabs) || !tabs.length) {
    return null;
  }
  return tabs.find((tab) => tab.active) || tabs[0] || null;
}

async function waitForTabComplete(tabId, timeoutMs = 15000) {
  const existing = await chrome.tabs.get(tabId);
  if (existing && existing.status === "complete") {
    return existing;
  }

  await new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      chrome.tabs.onUpdated.removeListener(handleUpdate);
      reject(new Error("La pestaña temporal no terminó de cargar a tiempo."));
    }, timeoutMs);

    function handleUpdate(updatedTabId, changeInfo, tab) {
      if (updatedTabId !== tabId || changeInfo.status !== "complete") {
        return;
      }
      clearTimeout(timeoutId);
      chrome.tabs.onUpdated.removeListener(handleUpdate);
      resolve(tab);
    }

    chrome.tabs.onUpdated.addListener(handleUpdate);
  });

  return chrome.tabs.get(tabId);
}

async function resolveTabForSource(source) {
  const existingTab = await queryExistingTab(source);
  if (existingTab) {
    return {
      tab: existingTab,
      temporary: false,
    };
  }

  const createdTab = await chrome.tabs.create({
    url: source.bootstrapUrl,
    active: false,
  });
  const readyTab = await waitForTabComplete(createdTab.id);
  return {
    tab: readyTab,
    temporary: true,
  };
}

async function withSourceTab(source, reader) {
  const context = await resolveTabForSource(source);
  try {
    return await reader(context.tab, context.temporary);
  } finally {
    if (context.temporary && context.tab && typeof context.tab.id === "number") {
      try {
        await chrome.tabs.remove(context.tab.id);
      } catch (_error) {
        // Ignore cleanup errors for temporary tabs.
      }
    }
  }
}

async function readLocalStorageSource(source) {
  return withSourceTab(source, async (tab, temporary) => {
    try {
      const response = await chrome.tabs.sendMessage(tab.id, {
        type: "READ_LOCAL_STORAGE",
        keys: [source.storageKey],
      });
      const value = String(response?.values?.[source.storageKey] || "").trim();
      return {
        id: source.id,
        label: source.label,
        sourceType: source.mode,
        storageKey: source.storageKey,
        value,
        found: Boolean(value),
        tab: serializeTab(tab),
        temporaryTab: temporary,
        error: value ? "" : "No se encontró valor en localStorage.",
      };
    } catch (error) {
      return {
        id: source.id,
        label: source.label,
        sourceType: source.mode,
        storageKey: source.storageKey,
        value: "",
        found: false,
        tab: serializeTab(tab),
        temporaryTab: temporary,
        error: normalizeError(error),
      };
    }
  });
}

async function readLoqueleoSource(source) {
  try {
    const cookies = await chrome.cookies.getAll({
      domain: source.cookieDomain,
    });
    const normalizedCookies = Array.isArray(cookies)
      ? cookies.map((cookie) => ({
        name: String(cookie.name || ""),
        value: String(cookie.value || ""),
        domain: String(cookie.domain || ""),
        path: String(cookie.path || ""),
        secure: Boolean(cookie.secure),
        httpOnly: Boolean(cookie.httpOnly),
        session: Boolean(cookie.session),
        sameSite: String(cookie.sameSite || ""),
      }))
      : [];
    const sessionCookie =
      normalizedCookies.find((cookie) => cookie.name === source.sessionCookieName) || null;
    return {
      id: source.id,
      label: source.label,
      sourceType: source.mode,
      sessionId: sessionCookie ? sessionCookie.value : "",
      cookieHeader: normalizedCookies
        .map((cookie) => `${cookie.name}=${cookie.value}`)
        .join("; "),
      cookies: normalizedCookies,
      found: Boolean(sessionCookie && sessionCookie.value),
      error:
        sessionCookie && sessionCookie.value
          ? ""
          : "No se encontró la cookie _session_id para Loqueleo.",
    };
  } catch (error) {
    return {
      id: source.id,
      label: source.label,
      sourceType: source.mode,
      sessionId: "",
      cookieHeader: "",
      cookies: [],
      found: false,
      error: normalizeError(error),
    };
  }
}

async function readSource(sourceId) {
  const source = SOURCE_CONFIG[sourceId];
  if (!source) {
    throw new Error(`Fuente no soportada: ${sourceId}`);
  }
  if (source.mode === "cookies") {
    return readLoqueleoSource(source);
  }
  return readLocalStorageSource(source);
}

async function readAllSources() {
  const entries = await Promise.all(
    Object.keys(SOURCE_CONFIG).map(async (sourceId) => [sourceId, await readSource(sourceId)])
  );
  return {
    readAt: new Date().toISOString(),
    sources: Object.fromEntries(entries),
  };
}

async function saveSnapshot(snapshot) {
  await chrome.storage.local.set({ lastReadSnapshot: snapshot });
  return snapshot;
}

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  (async () => {
    if (!message || typeof message.type !== "string") {
      sendResponse({ ok: false, error: "Mensaje no soportado." });
      return;
    }

    if (message.type === "READ_ALL_SOURCES") {
      const snapshot = await saveSnapshot(await readAllSources());
      sendResponse({ ok: true, data: snapshot });
      return;
    }

    if (message.type === "READ_SOURCE") {
      const sourceId = String(message.sourceId || "").trim().toLowerCase();
      const sourceData = await readSource(sourceId);
      const currentSnapshot = (await chrome.storage.local.get("lastReadSnapshot"))
        .lastReadSnapshot || {
        readAt: new Date().toISOString(),
        sources: {},
      };
      currentSnapshot.readAt = new Date().toISOString();
      currentSnapshot.sources = currentSnapshot.sources || {};
      currentSnapshot.sources[sourceId] = sourceData;
      await chrome.storage.local.set({ lastReadSnapshot: currentSnapshot });
      sendResponse({ ok: true, data: sourceData });
      return;
    }

    if (message.type === "GET_LAST_SNAPSHOT") {
      const snapshot = (await chrome.storage.local.get("lastReadSnapshot")).lastReadSnapshot || null;
      sendResponse({ ok: true, data: snapshot });
      return;
    }

    sendResponse({ ok: false, error: "Acción no soportada." });
  })().catch((error) => {
    sendResponse({ ok: false, error: normalizeError(error) });
  });

  return true;
});
