import type { CompressionAlgorithm } from "@amqp-contract/contract";
import { TechnicalError } from "@amqp-contract/core";
import { fromPromise, type AsyncResult } from "unthrown";
import { deflate, gzip } from "node:zlib";
import { promisify } from "node:util";
import { match } from "ts-pattern";

const gzipAsync = promisify(gzip);
const deflateAsync = promisify(deflate);

/**
 * Compress a buffer using the specified compression algorithm.
 *
 * @param buffer - The buffer to compress
 * @param algorithm - The compression algorithm to use
 * @returns An AsyncResult resolving to the compressed buffer or a TechnicalError
 *
 * @internal
 */
export function compressBuffer(
  buffer: Buffer,
  algorithm: CompressionAlgorithm,
): AsyncResult<Buffer, TechnicalError> {
  return match(algorithm)
    .with("gzip", () =>
      fromPromise(
        gzipAsync(buffer),
        (error) => new TechnicalError("Failed to compress with gzip", error),
      ),
    )
    .with("deflate", () =>
      fromPromise(
        deflateAsync(buffer),
        (error) => new TechnicalError("Failed to compress with deflate", error),
      ),
    )
    .exhaustive();
}
