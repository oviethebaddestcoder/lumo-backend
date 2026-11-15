// Visual preview and UI smoke test: opens every screen of the web app and
// of the mobile app (rendered with react-native-web) against a running
// stack with demo data, saves screenshots, and fails if any page throws or
// violates the Content-Security-Policy.
//
//   OUT=/tmp/lumo-preview/shots node shoot.mjs
// Env: GATEWAY (default http://localhost:18080), WEB (http://localhost:3000),
// APP (http://localhost:8090), VIDEO_ID and CREATOR_ID (from the demo seed).
import { createRequire } from "node:module";
import fs from "node:fs";

const require = createRequire(import.meta.url);
let chromium;
try {
  ({ chromium } = require("playwright"));
} catch {
  ({ chromium } = createRequire("/opt/npm-tools/node_modules/")("playwright"));
}

const GW = process.env.GATEWAY ?? "http://localhost:18080";
const WEB = process.env.WEB ?? "http://localhost:3000";
const APP = process.env.APP ?? "http://localhost:8090";
const OUT = process.env.OUT ?? "/tmp/lumo-preview/shots";
const VIDEO = process.env.VIDEO_ID;
const CREATOR = process.env.CREATOR_ID;
fs.mkdirSync(OUT, { recursive: true });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function otp(email) {
  const device_id = crypto.randomUUID();
  const h = { "content-type": "application/json" };
  await fetch(`${GW}/v1/auth/otp/send`, { method: "POST", headers: h, body: JSON.stringify({ channel: "email", target: email }) });
  const r = await fetch(`${GW}/v1/auth/otp/verify`, {
    method: "POST", headers: h,
    body: JSON.stringify({ channel: "email", target: email, code: "000000", device: { device_id, device_name: "Preview", platform: "android" } }),
  });
  const j = await r.json();
  if (!j.tokens) throw new Error(`sign-in ${email}: ${JSON.stringify(j)}`);
  return { ...j, device_id };
}

// Demo accounts (journey/demo_test.go): load1 = Ada (creator), load5 = Chioma
// (viewer, mobile), load6 = finance, load7 = a fan (web wallet).
const sess = {
  ada: await otp("load1@e2e.test"),
  chioma: await otp("load5@e2e.test"),
  finance: await otp("load6@e2e.test"),
  fan: await otp("load7@e2e.test"),
};

const results = [];
async function shot(page, name, title, url, { wait = 2500, after, variant = "" } = {}) {
  const errors = [];
  const onErr = (e) => errors.push(String(e.message ?? e).slice(0, 300));
  const onConsole = (m) => {
    if (/Content Security Policy|Refused to (load|execute)/.test(m.text())) errors.push("CSP: " + m.text().slice(0, 200));
  };
  page.on("pageerror", onErr);
  page.on("console", onConsole);
  try {
    if (url) await page.goto(url, { waitUntil: "networkidle", timeout: 30000 }).catch((e) => errors.push("navigation: " + e.message));
    if (after) await after(page);
    await sleep(wait);
    // Responsive check: nothing may push the page wider than the screen.
    const over = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth).catch(() => 0);
    if (over > 1) errors.push(`horizontal overflow: page is ${over}px wider than the ${page.viewportSize().width}px screen`);
    await page.screenshot({ path: `${OUT}/${name}.png` });
    results.push({ name, title, variant, url: page.url(), errors });
  } finally {
    page.off("pageerror", onErr);
    page.off("console", onConsole);
  }
}

const browser = await chromium.launch({
  executablePath: process.env.CHROME ?? (fs.existsSync("/usr/bin/google-chrome") ? "/usr/bin/google-chrome" : undefined),
  args: ["--autoplay-policy=no-user-gesture-required"],
});

// Each page is shot in every variant. The theme and size are switched on the
// same page (emulated prefers-color-scheme + viewport) so a session is used
// by one browser context only: refresh tokens rotate, and replaying an old
// one is treated as theft.
const WEB_VARIANTS = [
  { key: "light", label: "Desktop, light", width: 1440, height: 900, scheme: "light" },
  { key: "dark", label: "Desktop, dark", width: 1440, height: 900, scheme: "dark" },
  { key: "phone", label: "Phone browser, light", width: 390, height: 844, scheme: "light" },
];
const APP_VARIANTS = [
  { key: "light", label: "Phone, light", width: 390, height: 844, scheme: "light" },
  { key: "dark", label: "Phone, dark", width: 390, height: 844, scheme: "dark" },
  { key: "tablet", label: "Tablet, light", width: 820, height: 1180, scheme: "light" },
];
async function variant(page, v) {
  await page.emulateMedia({ colorScheme: v.scheme });
  await page.setViewportSize({ width: v.width, height: v.height });
}

// ---------- Web ----------
{
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1.5 });
  const page = await ctx.newPage();
  const as = async (k) => {
    await ctx.clearCookies();
    const t = sess[k].tokens;
    const host = new URL(WEB).hostname;
    await ctx.addCookies([
      { name: "lumo_at", value: t.access_token, domain: host, path: "/api", httpOnly: true },
      { name: "lumo_rt", value: t.refresh_token, domain: host, path: "/api", httpOnly: true },
      { name: "lumo_signed_in", value: "1", domain: host, path: "/" },
    ]);
  };
  const groups = [
    [null, [["01-landing", "/", "Landing page"], ["02-login", "/login", "Sign in (QR, phone, email, password, Google)"]]],
    ["ada", [
      ["03-studio-dashboard", "/studio", "Creator Studio: dashboard"], ["04-studio-live", "/studio/live", "Creator Studio: go live with OBS"],
      ["05-studio-videos", "/studio/videos", "Creator Studio: videos"], ["06-studio-earnings", "/studio/earnings", "Creator Studio: earnings and bank cash-out"],
      ["07-studio-audience", "/studio/audience", "Creator Studio: audience"], ["08-studio-moderation", "/studio/moderation", "Creator Studio: comment moderation"],
      ["09-studio-settings", "/studio/settings", "Creator Studio: settings"],
    ]],
    ["fan", [["10-wallet", "/wallet", "Buy coins (Paystack)"]]],
    ["finance", [["11-finance-payouts", "/admin/payouts", "Finance: cash-out approvals"]]],
    [null, [...(VIDEO ? [["12-share-video", `/v/${VIDEO}`, "Public video share page"]] : []), ["13-share-profile", "/@ada_cooks", "Public profile share page"]]],
  ];
  for (const [who, pages] of groups) {
    if (who) await as(who);
    else await ctx.clearCookies();
    for (const v of WEB_VARIANTS) {
      await variant(page, v);
      for (const [n, p, title] of pages) await shot(page, `web-${v.key}-${n}`, title, `${WEB}${p}`, { wait: 2500, variant: v.label });
    }
  }
  await ctx.close();
}

// ---------- Mobile app ----------
{
  const opts = { viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true };
  const out = await browser.newContext(opts);
  const op = await out.newPage();
  for (const v of APP_VARIANTS) {
    await variant(op, v);
    await shot(op, `app-${v.key}-01-sign-in`, "Sign in", `${APP}/sign-in`, { wait: 3000, variant: v.label });
  }
  await out.close();

  const ctx = await browser.newContext(opts);
  await ctx.addInitScript(([d, r]) => {
    // Only on first load: the app rotates the refresh token, and re-using an
    // old one is treated as theft (the session is revoked).
    if (!localStorage.getItem("lumo.device_id")) {
      localStorage.setItem("lumo.device_id", d);
      localStorage.setItem("lumo.refresh_token", r);
    }
  }, [sess.chioma.device_id, sess.chioma.tokens.refresh_token]);
  const page = await ctx.newPage();
  const screens = [
    ["02-for-you", "/", "For You feed"], ["03-live", "/live", "Live discovery"], ["04-watch", "/watch", "Watch (long videos)"],
    ["05-me", "/me", "Profile (Me)"], ["06-wallet", "/wallet", "Wallet, levels and cash-out"], ["07-inbox", "/inbox", "Activity inbox"],
    ["08-messages", "/messages", "Messages"], ["09-search", "/search", "Search people and hashtags"], ["10-tag", "/tag/cooking", "Hashtag page"],
    ["11-settings", "/settings", "Settings and appearance"], ["12-interests", "/settings/interests", "Content preferences"],
    ...(CREATOR ? [["13-creator-profile", `/user/${CREATOR}`, "Creator profile"]] : []), ["14-create", "/create", "Create: go live, short video, upload"],
    ["15-topics", "/topics", "Sign-up interest picker"],
  ];
  for (const v of APP_VARIANTS) {
    await variant(page, v);
    for (const [n, p, title] of screens) await shot(page, `app-${v.key}-${n}`, title, `${APP}${p}`, { wait: 3000, variant: v.label });
    await page.goto(`${APP}/messages`, { waitUntil: "networkidle" });
    await sleep(2500);
    const row = page.getByText("Ada's Kitchen").first();
    if (await row.count()) {
      await row.click();
      await shot(page, `app-${v.key}-16-chat`, "Chat", null, { wait: 3000, variant: v.label });
    }
  }
  await ctx.close();
}
await browser.close();

fs.writeFileSync(`${OUT}/index.json`, JSON.stringify(results, null, 2));
let failed = 0;
for (const r of results) {
  console.log(`${r.errors.length ? "FAIL" : "ok  "} ${r.name.padEnd(34)} ${r.title}${r.errors.length ? "\n     " + r.errors.join("\n     ") : ""}`);
  if (r.errors.length) failed++;
}
console.log(`\n${results.length} screens, ${failed} with errors. Screenshots in ${OUT}`);
process.exit(failed ? 1 : 0);
