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
async function shot(page, name, title, url, { wait = 2500, after } = {}) {
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
    await page.screenshot({ path: `${OUT}/${name}.png` });
    results.push({ name, title, url: page.url(), errors });
  } finally {
    page.off("pageerror", onErr);
    page.off("console", onConsole);
  }
}

const browser = await chromium.launch({
  executablePath: process.env.CHROME ?? (fs.existsSync("/usr/bin/google-chrome") ? "/usr/bin/google-chrome" : undefined),
  args: ["--autoplay-policy=no-user-gesture-required"],
});

// ---------- Web ----------
{
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1.5, colorScheme: "dark" });
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
  await shot(page, "web-01-landing", "Landing page", `${WEB}/`);
  await shot(page, "web-02-login", "Sign in (QR, phone, email, password, Google)", `${WEB}/login`);
  await as("ada");
  for (const [n, p, title] of [
    ["03-studio-dashboard", "", "Creator Studio: dashboard"], ["04-studio-live", "/live", "Creator Studio: go live with OBS"],
    ["05-studio-videos", "/videos", "Creator Studio: videos"], ["06-studio-earnings", "/earnings", "Creator Studio: earnings and bank cash-out"],
    ["07-studio-audience", "/audience", "Creator Studio: audience"], ["08-studio-moderation", "/moderation", "Creator Studio: comment moderation"],
    ["09-studio-settings", "/settings", "Creator Studio: settings"],
  ]) {
    await shot(page, `web-${n}`, title, `${WEB}/studio${p}`, { wait: 3000 });
  }
  await as("fan");
  await shot(page, "web-10-wallet", "Buy coins (Paystack)", `${WEB}/wallet`);
  await as("finance");
  await shot(page, "web-11-finance-payouts", "Finance: cash-out approvals", `${WEB}/admin/payouts`);
  const pub = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1.5, colorScheme: "dark" });
  const pp = await pub.newPage();
  if (VIDEO) await shot(pp, "web-12-share-video", "Public video share page", `${WEB}/v/${VIDEO}`, { wait: 3500 });
  await shot(pp, "web-13-share-profile", "Public profile share page", `${WEB}/@ada_cooks`);
  await pub.close();
  await ctx.close();
}

// ---------- Mobile app ----------
{
  const opts = { viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true, colorScheme: "dark" };
  const out = await browser.newContext(opts);
  await shot(await out.newPage(), "app-01-sign-in", "Sign in", `${APP}/sign-in`, { wait: 3000 });
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
    ["11-settings", "/settings", "Settings"], ["12-interests", "/settings/interests", "Content preferences"],
    ...(CREATOR ? [["13-creator-profile", `/user/${CREATOR}`, "Creator profile"]] : []), ["14-create", "/create", "Create: go live, short video, upload"],
    ["15-topics", "/topics", "Sign-up interest picker"],
  ];
  for (const [n, p, title] of screens) await shot(page, `app-${n}`, title, `${APP}${p}`, { wait: 3500 });
  await page.goto(`${APP}/messages`, { waitUntil: "networkidle" });
  await sleep(2500);
  const row = page.getByText("Ada's Kitchen").first();
  if (await row.count()) {
    await row.click();
    await shot(page, "app-16-chat", "Chat", null, { wait: 3000 });
  }
  await ctx.close();
}
await browser.close();

fs.writeFileSync(`${OUT}/index.json`, JSON.stringify(results, null, 2));
let failed = 0;
for (const r of results) {
  console.log(`${r.errors.length ? "FAIL" : "ok  "} ${r.name.padEnd(26)} ${r.title}${r.errors.length ? "\n     " + r.errors.join("\n     ") : ""}`);
  if (r.errors.length) failed++;
}
console.log(`\n${results.length} screens, ${failed} with errors. Screenshots in ${OUT}`);
process.exit(failed ? 1 : 0);
