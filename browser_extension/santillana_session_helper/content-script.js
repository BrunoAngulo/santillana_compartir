chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || message.type !== "READ_LOCAL_STORAGE") {
    return undefined;
  }

  const keys = Array.isArray(message.keys) ? message.keys : [];
  const values = {};
  for (const key of keys) {
    try {
      values[key] = String(window.localStorage.getItem(String(key)) || "").trim();
    } catch (_error) {
      values[key] = "";
    }
  }

  sendResponse({
    ok: true,
    href: window.location.href,
    title: document.title,
    values,
  });
  return undefined;
});
