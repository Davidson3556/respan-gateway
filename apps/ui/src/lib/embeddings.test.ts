import { describe, expect, it } from "vitest";
import { cosineSimilarity } from "./embeddings";

describe("cosineSimilarity", () => {
  it("is 1 for vectors in one direction, whatever their length", () => {
    expect(cosineSimilarity([1, 2, 3], [2, 4, 6])).toBeCloseTo(1);
  });

  it("is 0 for orthogonal vectors and -1 for opposed ones", () => {
    expect(cosineSimilarity([1, 0], [0, 1])).toBeCloseTo(0);
    expect(cosineSimilarity([1, 2], [-1, -2])).toBeCloseTo(-1);
  });

  it("is NaN when it cannot be told", () => {
    expect(cosineSimilarity([1, 2], [1])).toBeNaN();
    expect(cosineSimilarity([], [])).toBeNaN();
    expect(cosineSimilarity([0, 0], [1, 1])).toBeNaN();
  });
});
