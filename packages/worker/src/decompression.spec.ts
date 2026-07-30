import { promisify } from "node:util";
import { deflate, gzip, gzipSync } from "node:zlib";

import { describe, expect, it } from "vitest";

import { decompressBuffer } from "./decompression.js";

const gzipAsync = promisify(gzip);
const deflateAsync = promisify(deflate);

describe("Decompression utilities", () => {
  describe("decompressBuffer", () => {
    it("should return buffer as-is when no content-encoding is provided", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const result = await decompressBuffer(testData, undefined).get();

      expect(result).toEqual(testData);
    });

    it("should decompress gzip-compressed data", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = await gzipAsync(testData);

      const decompressed = await decompressBuffer(compressed, "gzip").get();

      expect(decompressed).toEqual(testData);
    });

    it("should decompress deflate-compressed data", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = await deflateAsync(testData);

      const decompressed = await decompressBuffer(compressed, "deflate").get();

      expect(decompressed).toEqual(testData);
    });

    it("should handle case-insensitive content-encoding", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));
      const compressed = await gzipAsync(testData);

      const decompressed = await decompressBuffer(compressed, "GZIP").get();

      expect(decompressed).toEqual(testData);
    });

    it("should surface a defect for unknown content-encoding with a helpful message", async () => {
      const testData = Buffer.from(JSON.stringify({ message: "Hello, World!" }));

      const result = await decompressBuffer(testData, "brotli");

      // An unsupported content-encoding is an infrastructure/producer fault, so
      // it lands in the defect channel (with a TechnicalError cause), not `E`.
      expect(result).toBeDefect();
      if (!result.isDefect()) throw new Error("expected Defect");
      const message = (result.cause as Error).message;
      expect(message).toContain('Unsupported content-encoding: "brotli"');
      expect(message).toContain("Supported encodings are: gzip, deflate");
      expect(message).toContain("Please check your publisher configuration");
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
      const decompressed = await decompressBuffer(compressed, "gzip").get();

      expect(decompressed).toEqual(largeData);
    });
  });
});

describe("decompression output cap (zip-bomb guard)", () => {
  it("INVARIANT: a payload expanding past the cap becomes a Defect instead of exhausting memory", async () => {
    // A few-KB gzip payload can expand to GBs before schema validation ever
    // runs. The cap turns that into the existing defect→DLQ path.
    const bomb = gzipSync(Buffer.alloc(1024 * 1024)); // 1 MiB of zeros, ~1 KiB compressed

    const result = await decompressBuffer(bomb, "gzip", { maxDecompressedBytes: 64 * 1024 });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(String((result.cause as Error).message)).toMatch(/decompress/i);
    }
  });

  it("a payload within the cap decompresses normally", async () => {
    const payload = Buffer.from(JSON.stringify({ ok: true }));
    const result = await decompressBuffer(gzipSync(payload), "gzip", {
      maxDecompressedBytes: 64 * 1024,
    });

    expect(result).toBeOk();
    if (result.isOk()) {
      expect(result.value.equals(payload)).toBe(true);
    }
  });
});
