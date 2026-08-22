"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");

const baseUrl = process.argv[2] || "http://127.0.0.1:18080";
const outputDir = process.argv[3] || "ui-audit";
const executablePath = process.env.CHROME_BIN || "/usr/bin/google-chrome";
const routes = [
  "/",
  "/dashboard",
  "/documents",
  "/sources",
  "/decon",
  "/datasets",
  "/post-training",
  "/mixture",
  "/as-of",
];

function slug(route) {
  return route === "/" ? "root" : route.slice(1).replaceAll("/", "-");
}

async function main() {
  fs.mkdirSync(outputDir, { recursive: true });
  const browser = await chromium.launch({
    executablePath,
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const results = [];

  for (const route of routes) {
    const page = await context.newPage();
    const consoleErrors = [];
    const pageErrors = [];
    const responses = [];
    page.on("console", (message) => {
      if (message.type() === "error") consoleErrors.push(message.text());
    });
    page.on("pageerror", (error) => pageErrors.push(String(error)));
    page.on("response", (response) => {
      const url = new URL(response.url());
      if (url.origin !== baseUrl) return;
      if (url.pathname.startsWith("/api/") || response.status() >= 400) {
        responses.push({ method: response.request().method(), path: url.pathname, status: response.status() });
      }
    });

    let navigationStatus = 0;
    let navigationError = null;
    try {
      const response = await page.goto(`${baseUrl}${route}`, {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      });
      navigationStatus = response ? response.status() : 0;
      await page.waitForTimeout(5_000);
    } catch (error) {
      navigationError = String(error);
    }

    const body = await page.locator("body").innerText().catch(() => "");
    const headings = await page.locator("h1, h2").allTextContents().catch(() => []);
    const links = await page.locator("a").allTextContents().catch(() => []);
    const buttons = await page.locator("button").allTextContents().catch(() => []);
    const inputs = await page
      .locator("input, select, textarea")
      .evaluateAll((elements) =>
        elements.map((element) => ({
          element: element.tagName.toLowerCase(),
          name: element.getAttribute("name"),
          placeholder: element.getAttribute("placeholder"),
          type: element.getAttribute("type"),
          value: element.value,
        })),
      )
      .catch(() => []);
    const screenshot = path.join(outputDir, `${slug(route)}.png`);
    await page.screenshot({ path: screenshot, fullPage: true }).catch(() => undefined);

    const failureMarkers = [
      "Application error",
      "Internal Server Error",
      "This page could not be found",
    ].filter((marker) => body.includes(marker));
    results.push({
      route,
      finalUrl: page.url(),
      navigationStatus,
      navigationError,
      title: await page.title().catch(() => ""),
      headings,
      links: links.map((value) => value.trim()).filter(Boolean),
      buttons: buttons.map((value) => value.trim()).filter(Boolean),
      inputs,
      bodyExcerpt: body.slice(0, 4_000),
      consoleErrors,
      pageErrors,
      responses,
      failureMarkers,
      screenshot: path.basename(screenshot),
    });
    await page.close();
  }

  await browser.close();
  const report = {
    auditedAt: new Date().toISOString(),
    baseUrl,
    routes: results,
  };
  fs.writeFileSync(path.join(outputDir, "report.json"), JSON.stringify(report, null, 2));
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);

  const broken = results.some(
    (result) =>
      result.navigationError ||
      result.navigationStatus >= 400 ||
      result.failureMarkers.length ||
      result.pageErrors.length,
  );
  if (broken) process.exitCode = 1;
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exitCode = 1;
});
