import { promisify } from "node:util";
import { deflate, gunzip, gzip, inflate } from "node:zlib";

import type { CompressionAlgorithm } from "@amqp-contract/contract";
import { fromPromise, fromSafeThrowable, OkAsync, type AsyncResult } from "unthrown";

import { TechnicalError } from "./errors.js";
import { safeJsonParse } from "./parsing.js";

/**
 * The message codec — the ONE place a payload becomes wire bytes and wire
 * bytes become a payload again. Publisher (client), RPC replies (client) and
 * consumer (worker) all go through it, so encoding, compression and the size
 * guard cannot drift apart between the two ends.
 */

/**
 * Default cap on the size of a single inbound message body, after
 * decompression. Applied to plain bodies as well as compressed ones: a
 * few-KB compressed payload can otherwise expand to gigabytes before schema
 * validation ever runs (a zip bomb), and an oversized plain body costs the
 * same memory to parse. Matches RabbitMQ 4's default `max_message_size`.
 * Over-cap messages surface through the defect channel and follow the
 * poison-message DLQ path.
 */
export const DEFAULT_MAX_MESSAGE_BYTES = 16 * 1024 * 1024;

/**
 * Supported content encodings, keyed by the header value they answer to.
 * Membership decides support, the key list renders the error message.
 */
const CODECS = {
  gzip: { compress: promisify(gzip), decompress: promisify(gunzip) },
  deflate: { compress: promisify(deflate), decompress: promisify(inflate) },
} as const satisfies Record<CompressionAlgorithm, unknown>;

/** Size options for the decode side. */
export type DecodeOptions = {
  /** Max body size in bytes, after decompression. Defaults to {@link DEFAULT_MAX_MESSAGE_BYTES}. */
  maxBytes?: number | undefined;
};

/**
 * Encode publishable content into the exact bytes that go on the wire:
 * Buffers pass through untouched (compressed payloads, retry republishing);
 * everything else is JSON-encoded. A non-serializable value (circular
 * references, BigInt) throws a {@link TechnicalError} — a programming fault,
 * adopted as a Defect by the caller's `fromSafeThrowable` boundary.
 */
export function encodeBody(content: unknown): Buffer {
  if (Buffer.isBuffer(content)) return content;
  try {
    return Buffer.from(JSON.stringify(content));
  } catch (error) {
    // oxlint-disable-next-line unthrown/no-throw -- known-technical precondition throw in a plain helper, adopted by the fromSafeThrowable boundary at the call sites
    throw new TechnicalError("Failed to JSON-encode message content", error);
  }
}

type EncodedMessage = { body: Buffer; contentEncoding: CompressionAlgorithm | undefined };

/**
 * JSON-encode a payload and, when asked, compress it. Returns the body and
 * the `contentEncoding` the message must carry. A failure is a Defect with a
 * {@link TechnicalError} cause.
 */
export function encodeMessage(
  content: unknown,
  compression?: CompressionAlgorithm,
): AsyncResult<EncodedMessage, never> {
  return fromSafeThrowable(() => encodeBody(content))()
    .toAsync()
    .flatMap((body): AsyncResult<EncodedMessage, never> =>
      compression === undefined
        ? OkAsync({ body, contentEncoding: undefined })
        : fromPromise(CODECS[compression].compress(body), (error, defect) =>
            defect(new TechnicalError(`Failed to compress with ${compression}`, error)),
          ).map((compressed) => ({ body: compressed, contentEncoding: compression })),
    );
}

/**
 * Undo `contentEncoding` and enforce the size cap. A plain body over the cap,
 * a decompressed body over the cap, an unknown encoding or a corrupt stream
 * is a Defect with a {@link TechnicalError} cause.
 */
export function decompressBuffer(
  buffer: Buffer,
  contentEncoding: string | undefined,
  options?: DecodeOptions,
): AsyncResult<Buffer, never> {
  const maxBytes = options?.maxBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
  const fail = (error: TechnicalError): AsyncResult<Buffer, never> =>
    fromSafeThrowable((): Buffer => {
      // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
      throw error;
    })().toAsync();

  if (!contentEncoding) {
    return buffer.length > maxBytes
      ? fail(
          new TechnicalError(
            `Message body of ${buffer.length} bytes exceeds the ${maxBytes}-byte limit`,
          ),
        )
      : OkAsync(buffer);
  }

  const encoding = contentEncoding.toLowerCase();
  const codec = Object.hasOwn(CODECS, encoding)
    ? CODECS[encoding as CompressionAlgorithm]
    : undefined;
  if (!codec) {
    return fail(
      new TechnicalError(
        `Unsupported content-encoding: "${contentEncoding}". ` +
          `Supported encodings are: ${Object.keys(CODECS).join(", ")}. ` +
          `Please check your publisher configuration.`,
      ),
    );
  }

  // zlib enforces the cap while inflating, so a bomb never materialises.
  return fromPromise(codec.decompress(buffer, { maxOutputLength: maxBytes }), (error, defect) =>
    defect(new TechnicalError(`Failed to decompress ${encoding}`, error)),
  );
}

/**
 * Wire bytes back to a payload: {@link decompressBuffer}, then JSON-parse. A
 * failure at either step is a Defect with a {@link TechnicalError} cause.
 */
export function decodeMessage(
  body: Buffer,
  contentEncoding: string | undefined,
  options?: DecodeOptions,
): AsyncResult<unknown, never> {
  return decompressBuffer(body, contentEncoding, options).flatMap((buffer) =>
    safeJsonParse(buffer, (error, defect) =>
      defect(new TechnicalError("Failed to parse JSON", error)),
    ),
  );
}
