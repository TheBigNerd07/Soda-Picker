const clock = document.getElementById("clock");

if (clock) {
  const timezone = clock.dataset.timezone;
  const initialIso = clock.dataset.iso;

  if (timezone && initialIso) {
    let current = new Date(initialIso);
    const formatter = new Intl.DateTimeFormat("en-US", {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
      timeZone: timezone,
    });

    const render = () => {
      clock.textContent = formatter.format(current);
      current = new Date(current.getTime() + 1000);
    };

    render();
    window.setInterval(render, 1000);
  }
}

const reminderConfig = document.getElementById("reminder-config");
const permissionButton = document.getElementById("notification-permission");
const installBanner = document.getElementById("install-banner");
const installBannerTitle = document.getElementById("install-banner-title");
const installBannerCopy = document.getElementById("install-banner-copy");
const installBannerAction = document.getElementById("install-banner-action");
const installBannerDismiss = document.getElementById("install-banner-dismiss");
const buildStamp = document.body?.dataset.build || "dev";
const INSTALL_DISMISS_KEY = "soda-picker-install-dismissed-v1";

const isStandalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
document.body.classList.toggle("standalone-app", isStandalone);

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register(`/service-worker.js?v=${encodeURIComponent(buildStamp)}`).catch(() => {
      // Ignore service worker registration failures and keep the app usable.
    });
  });
}

let deferredInstallPrompt = null;

const showInstallBanner = ({ title, copy, buttonLabel, onAction }) => {
  if (!installBanner || !installBannerTitle || !installBannerCopy || !installBannerAction || !installBannerDismiss) {
    return;
  }
  if (isStandalone || window.localStorage.getItem(INSTALL_DISMISS_KEY) === "true") {
    return;
  }

  installBannerTitle.textContent = title;
  installBannerCopy.textContent = copy;
  installBannerAction.textContent = buttonLabel;
  installBanner.hidden = false;

  installBannerAction.onclick = onAction;
  installBannerDismiss.onclick = () => {
    window.localStorage.setItem(INSTALL_DISMISS_KEY, "true");
    installBanner.hidden = true;
  };
};

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  deferredInstallPrompt = event;
  showInstallBanner({
    title: "Install Soda Picker",
    copy: "Install Soda Picker so it launches in its own window and feels closer to a native app.",
    buttonLabel: "Install app",
    onAction: async () => {
      if (!deferredInstallPrompt) {
        return;
      }
      deferredInstallPrompt.prompt();
      await deferredInstallPrompt.userChoice;
      deferredInstallPrompt = null;
      if (installBanner) {
        installBanner.hidden = true;
      }
    },
  });
});

window.addEventListener("appinstalled", () => {
  deferredInstallPrompt = null;
  if (installBanner) {
    installBanner.hidden = true;
  }
  document.body.classList.add("standalone-app");
});

const isIos = /iphone|ipad|ipod/i.test(window.navigator.userAgent);
const isSafari = /safari/i.test(window.navigator.userAgent) && !/crios|fxios|edgios/i.test(window.navigator.userAgent);

if (isIos && isSafari && !isStandalone) {
  showInstallBanner({
    title: "Add Soda Picker to your Home Screen",
    copy: "In Safari, tap Share, then choose Add to Home Screen. It will open in standalone mode like an app.",
    buttonLabel: "Got it",
    onAction: () => {
      window.localStorage.setItem(INSTALL_DISMISS_KEY, "true");
      if (installBanner) {
        installBanner.hidden = true;
      }
    },
  });
}

const getTimeParts = (timezone) => {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  const parts = formatter.formatToParts(new Date());
  const map = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      map[part.type] = part.value;
    }
  }
  return map;
};

if (reminderConfig && permissionButton) {
  const enabled = reminderConfig.dataset.enabled === "true";
  const reminderTime = reminderConfig.dataset.reminderTime || "";
  const timezone = reminderConfig.dataset.timezone || "UTC";

  if (!enabled) {
    permissionButton.textContent = "Reminder disabled in settings";
    permissionButton.disabled = true;
  } else if (!("Notification" in window)) {
    permissionButton.textContent = "Browser notifications unsupported";
    permissionButton.disabled = true;
  } else {
    const updatePermissionButton = () => {
      if (Notification.permission === "granted") {
        permissionButton.textContent = "Browser reminders enabled";
        permissionButton.disabled = true;
      } else if (Notification.permission === "denied") {
        permissionButton.textContent = "Browser reminders blocked";
        permissionButton.disabled = true;
      } else {
        permissionButton.textContent = "Enable browser reminder";
        permissionButton.disabled = false;
      }
    };

    permissionButton.addEventListener("click", async () => {
      const result = await Notification.requestPermission();
      if (result === "granted") {
        new Notification("Soda Picker", {
          body: "Browser reminders are on for this tab.",
        });
      }
      updatePermissionButton();
    });

    updatePermissionButton();

    const [targetHour, targetMinute] = reminderTime.split(":").map((value) => Number.parseInt(value, 10));
    const maybeNotify = () => {
      if (Notification.permission !== "granted") {
        return;
      }
      if (Number.isNaN(targetHour) || Number.isNaN(targetMinute)) {
        return;
      }

      const parts = getTimeParts(timezone);
      const currentHour = Number.parseInt(parts.hour, 10);
      const currentMinute = Number.parseInt(parts.minute, 10);
      const dateKey = `${parts.year}-${parts.month}-${parts.day}`;
      const reminderKey = `soda-picker-reminder-${dateKey}-${reminderTime}`;

      if (currentHour === targetHour && currentMinute === targetMinute) {
        if (window.localStorage.getItem(reminderKey) === "sent") {
          return;
        }
        window.localStorage.setItem(reminderKey, "sent");
        new Notification("Soda window is open", {
          body: "Soda Picker says your reminder window just opened.",
        });
      }
    };

    maybeNotify();
    window.setInterval(maybeNotify, 30000);
  }
}
