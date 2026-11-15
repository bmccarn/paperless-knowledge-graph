import { defineConfig } from "playwright/test";
import path from "node:path";
import { visualMode } from "./visual-policy.mjs";

const mode = visualMode();
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || ".browser-artifacts/visual");
export default defineConfig({
  testDir: ".",
  testMatch: "visual.spec.mjs",
  workers: 1,
  retries: 0,
  timeout: 45000,
  outputDir: path.join(artifacts, "results"),
  reporter: [["list"], ["json", { outputFile: path.join(artifacts, "report.json") }], ["html", { outputFolder: path.join(artifacts, "comparison"), open: "never" }]],
  snapshotPathTemplate: "{testDir}/baselines/{projectName}/{arg}{ext}",
  updateSnapshots: mode === "update" ? "all" : "none",
  use: {
    baseURL: process.env.UI_TEST_BASE_URL,
    browserName: "chromium",
    deviceScaleFactor: 1,
    locale: "en-US",
    timezoneId: "UTC",
    reducedMotion: "reduce",
  },
  projects: [
    { name: "desktop-dark", use: { viewport: { width: 1440, height: 900 }, colorScheme: "dark" } },
    { name: "desktop-light", use: { viewport: { width: 1440, height: 900 }, colorScheme: "light" } },
    { name: "phone-dark", use: { viewport: { width: 390, height: 844 }, colorScheme: "dark" } },
    { name: "phone-light", use: { viewport: { width: 390, height: 844 }, colorScheme: "light" } },
  ],
});
