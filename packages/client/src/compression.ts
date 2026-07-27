import { promisify } from "node:util";
import { deflate, gzip } from "node:zlib";

import type { CompressionAlgorithm } from "@amqp-contract/contract";
import { TechnicalError } from "@amqp-contract/core";
import { fromPromise, match, type AsyncResult } from "unthrown";

const gzipAsync = promisify(gzip);
const deflateAsync = promisify(deflate);

/**
 * Compress a buffer using the specified compression algorithm.
 *
 * @param buffer - The buffer to compress
 * @param algorithm - The compression algorithm to use
 * @returns An AsyncResult resolving to the compressed buffer. A compression
 *   failure is an infrastructure fault, so it surfaces through the `Defect`
 *   channel (with a {@link TechnicalError} cause), never a modeled `Err`.
 *
 * @internal
 */
export function compressBuffer(
  buffer: Buffer,
  algorithm: CompressionAlgorithm,
): AsyncResult<Buffer, never> {
  return match(algorithm)
    .with("gzip", () =>
      fromPromise(gzipAsync(buffer), (error, defect) =>
        defect(new TechnicalError("Failed to compress with gzip", error)),
      ),
    )
    .with("deflate", () =>
      fromPromise(deflateAsync(buffer), (error, defect) =>
        defect(new TechnicalError("Failed to compress with deflate", error)),
      ),
    )
    .exhaustive();
}
