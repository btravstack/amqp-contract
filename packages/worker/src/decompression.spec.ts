import { deflate, gzip } from "node:zlib";
import { describe, expect, it } from "vitest";
import { decompressBuffer } from "./decompression.js";
import { promisify } from "node:util";

const gzipAsync = promisify(gzip);
const deflateAsync = promisify(deflate);

describe("Decompression utilities", () => {
  describe("decompressBuffer", () => {
    it("should return buffer as-is when no content-encoding is provided", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const result = await decompressBuffer(testData, undefined).getOrElse((e) => {
        throw e;
      });

      expect(result).toEqual(testData);
    });

    it("should decompress gzip-compressed data", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = await gzipAsync(testData);

      const decompressed = await decompressBuffer(compressed, "gzip").getOrElse((e) => {
        throw e;
      });

      expect(decompressed).toEqual(testData);
    });

    it("should decompress deflate-compressed data", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = await deflateAsync(testData);

      const decompressed = await decompressBuffer(compressed, "deflate").getOrElse((e) => {
        throw e;
      });

      expect(decompressed).toEqual(testData);
    });

    it("should handle case-insensitive content-encoding", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = await gzipAsync(testData);

      const decompressed = await decompressBuffer(compressed, "GZIP").getOrElse((e) => {
        throw e;
      });

      expect(decompressed).toEqual(testData);
    });

    it("should return error for unknown content-encoding with helpful message", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));

      const result = await decompressBuffer(testData, "brotli");

      expect(result).toBeErr();
      if (!result.isErr()) throw new Error("expected Err");
      const error = result.error;
      expect(error.message).toContain('Unsupported content-encoding: "brotli"');
      expect(error.message).toContain("Supported encodings are: gzip, deflate");
      expect(error.message).toContain("Please check your publisher configuration");
    });

    it("should decompress large data correctly", async () => {
      const largeData = Buffer.from(
        JSON.stringify({
          items: Array.from({ length: 1000 }, (_, i) => ({
            id: i,
            name: "Item " + i,
            description: "This is a test item with some repetitive text",
          })),
        }),
      );

      const compressed = await gzipAsync(largeData);
      const decompressed = await decompressBuffer(compressed, "gzip").getOrElse((e) => {
        throw e;
      });

      expect(decompressed).toEqual(largeData);
    });
  });
});
