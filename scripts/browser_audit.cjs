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
  "/datasets",
  "/post-training",
  "/mixture",
  "/as-of",
];
const now = new Date();
const thirtyDaysAgo = new Date(now);
thirtyDaysAgo.setUTCDate(thirtyDaysAgo.getUTCDate() - 30);
const apiProbes = [
  "/api/health",
  "/api/dashboard",
  "/api/activity?window=5m",
  "/api/documents?limit=5&include_fixtures=true",
  "/api/documents/facets?include_fixtures=true",
  "/api/sources",
  `/api/datasets/summary?date_from=${encodeURIComponent(thirtyDaysAgo.toISOString())}&date_to=${encodeURIComponent(now.toISOString())}&route=pretrain&route=posttrain_candidate&include_structured=true`,
  `/api/as-of?ts=${encodeURIComponent(now.toISOString())}`,
  "/api/foundry/dashboard",
  "/api/foundry/artifacts?limit=5",
  "/api/foundry/activity?window=5m",
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
    const bodyFontFamily = await page
      .locator("body")
      .evaluate((element) => getComputedStyle(element).fontFamily)
      .catch(() => "");
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
      bodyFontFamily,
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

  const probes = [];
  for (const probe of apiProbes) {
    try {
      const response = await context.request.get(`${baseUrl}${probe}`, { timeout: 30_000 });
      const contentType = response.headers()["content-type"] || "";
      const body = await response.text();
      let validJson = false;
      if (contentType.includes("application/json")) {
        try {
          JSON.parse(body);
          validJson = true;
        } catch {
          validJson = false;
        }
      }
      probes.push({
        path: probe,
        status: response.status(),
        contentType,
        validJson,
        bodyExcerpt: body.slice(0, 1_000),
      });
    } catch (error) {
      probes.push({
        path: probe,
        status: 0,
        contentType: "",
        validJson: false,
        error: String(error),
      });
    }
  }

  // Keep full content inspection evidence in the short-lived audit artifact,
  // not console logs or the source repository. This issues GET requests only.
  const content = { documents: [], artifacts: [], errors: [] };
  async function readJson(route) {
    const response = await context.request.get(`${baseUrl}${route}`, { timeout: 60_000 });
    if (!response.ok()) throw new Error(`${route}: HTTP ${response.status()}`);
    return response.json();
  }
  for (const source of ["arxiv-html-fetcher", "hf-models", "hf-datasets"]) {
    for (const route of ["pretrain", "posttrain_candidate", "quarantine"]) {
      if (source !== "arxiv-html-fetcher" && route === "posttrain_candidate") continue;
      try {
        const query = new URLSearchParams({ source, route, page_size: "3", sort: "newest" });
        const listing = await readJson(`/api/documents?${query}`);
        for (const row of listing.items || []) {
          content.documents.push(await readJson(`/api/documents/${encodeURIComponent(row.doc_id)}`));
        }
      } catch (error) { content.errors.push(String(error)); }
    }
  }
  try {
    const listing = await readJson("/api/foundry/artifacts?limit=500");
    const counts = new Map();
    for (const artifact of listing.items || []) {
      const key = `${artifact.kind}:${artifact.status}`;
      if ((counts.get(key) || 0) >= 3) continue;
      counts.set(key, (counts.get(key) || 0) + 1);
      try {
        content.artifacts.push(await readJson(`/api/foundry/artifacts/${encodeURIComponent(artifact.artifact_id)}/inspect`));
      } catch (error) { content.errors.push(String(error)); }
    }
  } catch (error) { content.errors.push(String(error)); }
  fs.writeFileSync(path.join(outputDir, "content-inspection.json"), JSON.stringify(content, null, 2));
  await browser.close();
  const report = {
    auditedAt: new Date().toISOString(),
    baseUrl,
    routes: results,
    apiProbes: probes,
    contentInspection: { documents: content.documents.length, artifacts: content.artifacts.length, errors: content.errors },
  };
  fs.writeFileSync(path.join(outputDir, "report.json"), JSON.stringify(report, null, 2));
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);

  const broken = results.some(
    (result) =>
      result.navigationError ||
      result.navigationStatus >= 400 ||
      result.failureMarkers.length ||
      result.pageErrors.length ||
      !result.bodyFontFamily ||
      /times new roman|(^|,\s*)serif($|,)/i.test(result.bodyFontFamily) ||
      result.responses.some((response) => response.status >= 400),
  ) || probes.some((probe) => probe.status < 200 || probe.status >= 300 || !probe.validJson);
  if (broken) process.exitCode = 1;
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exitCode = 1;
});
