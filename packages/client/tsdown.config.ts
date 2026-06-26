import { defineConfig } from "tsdown";

export default defineConfig({
  // Prevent unthrown types from being inlined into the declaration files.
  // This ensures type compatibility across packages that re-export them.
  external: ["unthrown"],
  // Suppress warnings about bundled dependencies in declaration files
  inlineOnly: false,
});
