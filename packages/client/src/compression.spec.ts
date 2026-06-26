import { describe, expect, it } from "vitest";

import { compressBuffer } from "./compression.js";

describe("Compression utilities", () => {
  describe("compressBuffer", () => {
    it("should compress and decompress data with gzip algorithm", async () => {
      const { gunzip } = await import("node:zlib");
      const { promisify } = await import("node:util");
      const gunzipAsync = promisify(gunzip);

      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = (await compressBuffer(testData, "gzip")).unwrap();
      const decompressed = await gunzipAsync(compressed);

      expect(decompressed).toEqual(testData);
    });

    it("should compress and decompress data with deflate algorithm", async () => {
      const { inflate } = await import("node:zlib");
      const { promisify } = await import("node:util");
      const inflateAsync = promisify(inflate);

      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = (await compressBuffer(testData, "deflate")).unwrap();
      const decompressed = await inflateAsync(compressed);

      expect(decompressed).toEqual(testData);
    });

    it("should compress large data efficiently", async () => {
      // Create a large JSON object with repetitive data
      const largeData = Buffer.from(
        JSON.stringify({
          items: Array.from({ length: 1000 }, (_, i) => ({
            id: i,
            name: "Item " + i,
            description: "This is a test item with some repetitive text",
          })),
        }),
      );

      const compressed = (await compressBuffer(largeData, "gzip")).unwrap();

      // Compressed data should be significantly smaller
      expect(compressed.length).toBeLessThan(largeData.length);
    });
  });
});
