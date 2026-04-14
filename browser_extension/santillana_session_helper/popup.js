const SOURCE_META = {
  pegasus: {
    label: "Procesos Pegasus",
    primaryField: "value",
    emptyValue: "",
  },
  richmond: {
    label: "Richmond Studio",
    primaryField: "value",
    emptyValue: "",
  },
  loqueleo: {
    label: "Loqueleo",
    primaryField: "sessionId",
    emptyValue: "",
  },
  ipa: {
    label: "IPA",
    primaryField: "sessionValue",
    emptyValue: "",
  },
};

let currentSnapshot = null;

function getEl(id) {
  return document.getElementById(id);
}

function getSourceCard(sourceId) {
  return document.querySelector(`[data-source-card="${sourceId}"]`);
}

function formatDate(dateText) {
  if (!dateText) {
    return "Sin lecturas todavía.";
  }
  const parsed = new Date(dateText);
  if (Number.isNaN(parsed.getTime())) {
    return dateText;
  }
  return parsed.toLocaleString();
}

function setToolbarDisabled(disabled) {
  getEl("refresh-btn").disabled = disabled;
  getEl("copy-json-btn").disabled = disabled;
}

function setStatus(text) {
  getEl("status-text").textContent = text;
}

function describeSourceState(sourceData) {
  if (!sourceData) {
    return {
      text: "Sin datos disponibles.",
      className: "card__state card__state--warn",
    };
  }
  if (sourceData.found) {
    return {
      text: "Dato encontrado.",
      className: "card__state card__state--ok",
    };
  }
  if (sourceData.error) {
    return {
      text: sourceData.error,
      className: "card__state card__state--error",
    };
  }
  return {
    text: "No se encontró información.",
    className: "card__state card__state--warn",
  };
}

function renderSource(sourceId, sourceData) {
  const card = getSourceCard(sourceId);
  if (!card) {
    return;
  }

  const meta = SOURCE_META[sourceId];
  const stateEl = card.querySelector('[data-role="state"]');
  const valueEl = card.querySelector('[data-role="value"]');
  const metaEl = card.querySelector('[data-role="meta"]');
  const detailsEl = card.querySelector('[data-role="details"]');
  const detailsContentEl = card.querySelector('[data-role="details-content"]');

  const stateInfo = describeSourceState(sourceData);
  stateEl.textContent = stateInfo.text;
  stateEl.className = stateInfo.className;

  const primaryValue =
    sourceData && typeof sourceData[meta.primaryField] === "string"
      ? sourceData[meta.primaryField]
      : meta.emptyValue;
  valueEl.value = primaryValue;

  if (sourceData?.sourceType === "cookies") {
    const cookies = Array.isArray(sourceData?.cookies) ? sourceData.cookies : [];
    const detailsText = cookies.length
      ? JSON.stringify(cookies, null, 2)
      : "No hay cookies detectadas.";
    detailsContentEl.textContent = detailsText;
    detailsEl.hidden = false;
    metaEl.textContent = sourceData?.cookieHeader
      ? `Cookie header disponible. Total cookies: ${cookies.length}.`
      : "No se detectó cookie header.";
    return;
  }

  const tabMeta = sourceData?.tab;
  const tabLabel = tabMeta && tabMeta.url ? `${tabMeta.title || "Pestaña"}\n${tabMeta.url}` : "";
  const temporaryNote = sourceData?.temporaryTab
    ? "Se abrió una pestaña temporal para leer el valor."
    : "";
  metaEl.textContent = [tabLabel, temporaryNote].filter(Boolean).join("\n");
}

function renderSnapshot(snapshot) {
  currentSnapshot = snapshot;
  const sources = snapshot?.sources || {};
  renderSource("pegasus", sources.pegasus);
  renderSource("richmond", sources.richmond);
  renderSource("loqueleo", sources.loqueleo);
  renderSource("ipa", sources.ipa);
  setStatus(`Última lectura: ${formatDate(snapshot?.readAt)}`);
}

async function sendRuntimeMessage(message) {
  const response = await chrome.runtime.sendMessage(message);
  if (!response || !response.ok) {
    throw new Error(response?.error || "No se pudo completar la acción.");
  }
  return response.data;
}

async function loadLastSnapshot() {
  try {
    const snapshot = await sendRuntimeMessage({ type: "GET_LAST_SNAPSHOT" });
    if (snapshot) {
      renderSnapshot(snapshot);
      return;
    }
    setStatus("No hay lecturas guardadas todavía.");
  } catch (error) {
    setStatus(`No se pudo cargar el estado inicial: ${error.message}`);
  }
}

async function refreshSnapshot() {
  setToolbarDisabled(true);
  setStatus("Leyendo datos del navegador...");
  try {
    const snapshot = await sendRuntimeMessage({ type: "READ_ALL_SOURCES" });
    renderSnapshot(snapshot);
  } catch (error) {
    setStatus(`Error al leer datos: ${error.message}`);
  } finally {
    setToolbarDisabled(false);
  }
}

async function copyText(text, successMessage) {
  if (!String(text || "").trim()) {
    setStatus("No hay valor para copiar.");
    return;
  }
  try {
    await navigator.clipboard.writeText(String(text));
    setStatus(successMessage);
  } catch (error) {
    setStatus(`No se pudo copiar: ${error.message}`);
  }
}

document.addEventListener("click", async (event) => {
  const copyButton = event.target.closest("[data-copy-source]");
  if (!copyButton) {
    return;
  }

  const sourceId = String(copyButton.dataset.copySource || "").trim();
  const fieldName = String(copyButton.dataset.copyField || "").trim();
  const sourceData = currentSnapshot?.sources?.[sourceId];
  const value = sourceData ? sourceData[fieldName] : "";
  await copyText(value, `${SOURCE_META[sourceId]?.label || sourceId}: valor copiado.`);
});

getEl("refresh-btn").addEventListener("click", refreshSnapshot);
getEl("copy-json-btn").addEventListener("click", async () => {
  const text = currentSnapshot ? JSON.stringify(currentSnapshot, null, 2) : "";
  await copyText(text, "Snapshot completo copiado en JSON.");
});

loadLastSnapshot();
