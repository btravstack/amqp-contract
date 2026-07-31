import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { MATCH_CORPUS } from "./match-corpus.js";
import { _internal_matchesTopicPattern } from "./topic-match.js";

describe("_internal_matchesTopicPattern", () => {
  describe("shared corpus", () => {
    for (const { key, pattern, matches } of MATCH_CORPUS) {
      it(`${matches ? "matches" : "does not match"} key "${key}" against pattern "${pattern}"`, () => {
        expect(_internal_matchesTopicPattern(key, pattern)).toBe(matches);
      });
    }
  });

  describe("properties", () => {
    // Words are non-empty, wildcard-free AMQP-ish tokens.
    const word = fc.stringMatching(/^[a-z0-9_-]{1,6}$/);
    const key = fc.array(word, { minLength: 1, maxLength: 4 }).map((w) => w.join("."));

    it("a wildcard-free pattern matches exactly its own key", () => {
      fc.assert(
        fc.property(key, (k) => {
          expect(_internal_matchesTopicPattern(k, k)).toBe(true);
        }),
      );
    });

    it("'#' alone matches every key", () => {
      fc.assert(
        fc.property(key, (k) => {
          expect(_internal_matchesTopicPattern(k, "#")).toBe(true);
        }),
      );
    });

    it("an all-'*' pattern matches iff the word counts are equal", () => {
      fc.assert(
        fc.property(key, fc.integer({ min: 1, max: 6 }), (k, starCount) => {
          const pattern = Array.from({ length: starCount }, () => "*").join(".");
          const expected = k.split(".").length === starCount;
          expect(_internal_matchesTopicPattern(k, pattern)).toBe(expected);
        }),
      );
    });

    it("appending '.#' to a matching pattern keeps it matching", () => {
      fc.assert(
        fc.property(key, (k) => {
          expect(_internal_matchesTopicPattern(k, `${k}.#`)).toBe(true);
        }),
      );
    });

    it("is deterministic", () => {
      fc.assert(
        fc.property(key, key, (k, p) => {
          expect(_internal_matchesTopicPattern(k, p)).toBe(_internal_matchesTopicPattern(k, p));
        }),
      );
    });
  });
});
