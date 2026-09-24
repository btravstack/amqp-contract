import { promisify } from "node:util";
import { deflate, gunzip, gzip, gzipSync, inflate } from "node:zlib";

import { describe, expect, it } from "vitest";

import {
  decodeMessage,
  decompressBuffer,
  DEFAULT_MAX_MESSAGE_BYTES,
  encodeMessage,
} from "./codec.js";
import { TechnicalError } from "./errors.js";

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

    const result = await decompressBuffer(bomb, "gzip", { maxBytes: 64 * 1024 });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(String((result.cause as Error).message)).toMatch(/decompress/i);
    }
  });

  it("a payload within the cap decompresses normally", async () => {
    const payload = Buffer.from(JSON.stringify({ ok: true }));
    const result = await decompressBuffer(gzipSync(payload), "gzip", {
      maxBytes: 64 * 1024,
    });

    expect(result).toBeOk();
    if (result.isOk()) {
      expect(result.value.equals(payload)).toBe(true);
    }
  });
});

describe("uncompressed size cap", () => {
  it("defaults to 16 MiB", () => {
    expect(DEFAULT_MAX_MESSAGE_BYTES).toBe(16 * 1024 * 1024);
  });

  it("INVARIANT: a plain body over the cap is a Defect, never parsed", async () => {
    const body = Buffer.from(JSON.stringify({ padding: "x".repeat(2048) }));

    const result = await decodeMessage(body, undefined, { maxBytes: 1024 });

    expect(result).toBeDefectWith(expect.objectContaining({ constructor: TechnicalError }));
    if (result.isDefect()) expect((result.cause as Error).message).toContain("1024-byte limit");
  });
});

describe("encodeMessage / decodeMessage round trip", () => {
  it.for([undefined, "gzip", "deflate"] as const)(
    "round-trips a payload with compression=%s",
    async (compression) => {
      const payload = { orderId: "1", items: [1, 2, 3] };

      const { body, contentEncoding } = await encodeMessage(payload, compression).get();
      const decoded = await decodeMessage(body, contentEncoding).get();

      expect([contentEncoding, decoded]).toEqual([compression, payload]);
    },
  );

  it("compresses with the real zlib formats", async () => {
    const payload = { message: "Hello, World!" };
    const expected = Buffer.from(JSON.stringify(payload));

    const gz = await encodeMessage(payload, "gzip").get();
    const df = await encodeMessage(payload, "deflate").get();

    expect([await promisify(gunzip)(gz.body), await promisify(inflate)(df.body)]).toEqual([
      expected,
      expected,
    ]);
  });

  it("passes a Buffer through byte-for-byte", async () => {
    const raw = Buffer.from([0, 1, 2, 255]);

    const { body } = await encodeMessage(raw).get();

    expect(body.equals(raw)).toBe(true);
  });

  it("an unencodable payload is a Defect", async () => {
    const circular: Record<string, unknown> = {};
    circular["self"] = circular;

    expect(await encodeMessage(circular)).toBeDefect();
  });
});
