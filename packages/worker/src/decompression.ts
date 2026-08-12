import { promisify } from "node:util";
import { gunzip, inflate } from "node:zlib";

import { TechnicalError } from "@amqp-contract/core";
import { fromPromise, fromSafeThrowable, OkAsync, type AsyncResult } from "unthrown";

/**
 * Default cap on the decompressed size of a single message. A few-KB
 * malicious or corrupt payload can otherwise expand to gigabytes before
 * schema validation ever runs (a zip bomb), taking the worker down — and,
 * after redelivery, the next worker. Over-limit payloads surface through the
 * defect channel and follow the existing poison-message DLQ path.
 */
export const DEFAULT_MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024;

/**
 * Supported content encodings, keyed by the header value they answer to. The
 * map is the whole encoding-support story: membership decides whether an
 * encoding is supported, the key list renders the error message, and the value
 * does the work.
 */
const DECOMPRESSORS = {
  gzip: promisify(gunzip),
  deflate: promisify(inflate),
} as const;

/**
 * Decompress a buffer based on the content-encoding header.
 *
 * @param buffer - The buffer to decompress
 * @param contentEncoding - The content-encoding header value (e.g., 'gzip', 'deflate')
 * @returns An AsyncResult resolving to the decompressed buffer. Decompression
 *   failures are infrastructure faults, so they surface through the `Defect`
 *   channel (with a {@link TechnicalError} cause), never a modeled `Err`.
 *
 * @internal
 */
export function decompressBuffer(
  buffer: Buffer,
  contentEncoding: string | undefined,
  options?: { maxDecompressedBytes?: number | undefined },
): AsyncResult<Buffer, never> {
  if (!contentEncoding) {
    return OkAsync(buffer);
  }
  const maxOutputLength = options?.maxDecompressedBytes ?? DEFAULT_MAX_DECOMPRESSED_BYTES;
  const encoding = contentEncoding.toLowerCase();
  const decompress = Object.hasOwn(DECOMPRESSORS, encoding)
    ? DECOMPRESSORS[encoding as keyof typeof DECOMPRESSORS]
    : undefined;

  if (!decompress) {
    return fromSafeThrowable((): Buffer => {
      // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
      throw new TechnicalError(
        `Unsupported content-encoding: "${contentEncoding}". ` +
          `Supported encodings are: ${Object.keys(DECOMPRESSORS).join(", ")}. ` +
          `Please check your publisher configuration.`,
      );
    })().toAsync();
  }

  return fromPromise(decompress(buffer, { maxOutputLength }), (error, defect) =>
    defect(new TechnicalError(`Failed to decompress ${encoding}`, error)),
  );
}
