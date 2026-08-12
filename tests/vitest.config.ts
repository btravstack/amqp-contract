import { defineConfig } from "vitest/config";

import { sharedVitestConfig } from "../vitest.shared.js";

export default defineConfig(sharedVitestConfig({ integration: true }));
