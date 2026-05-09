import { describe, expect, it } from "vitest";
import { formatRtt } from "./formatRtt";

describe("formatRtt", () => {
  it("ms scale, sub-10 ms, 2 decimals", () => {
    expect(formatRtt(1.41)).toEqual({ value: "1.41", unit: "ms" });
    expect(formatRtt(0.5)).toEqual({ value: "0.50", unit: "ms" });
  });

  it("ms scale, 10–100 ms, 1 decimal", () => {
    expect(formatRtt(12.5)).toEqual({ value: "12.5", unit: "ms" });
    expect(formatRtt(99.94)).toEqual({ value: "99.9", unit: "ms" });
  });

  it("ms scale, 100–1000 ms, integer", () => {
    expect(formatRtt(123.7)).toEqual({ value: "124", unit: "ms" });
    expect(formatRtt(999.4)).toEqual({ value: "999", unit: "ms" });
  });

  it("flips to s at 1000 ms (the case that used to overflow)", () => {
    expect(formatRtt(1033.49)).toEqual({ value: "1.03", unit: "s" });
    expect(formatRtt(9999)).toEqual({ value: "10.0", unit: "s" });
  });

  it("keeps 4 numeric chars at higher seconds", () => {
    expect(formatRtt(12_345)).toEqual({ value: "12.3", unit: "s" });
    expect(formatRtt(123_456)).toEqual({ value: "123", unit: "s" });
  });

  it("guards bogus inputs", () => {
    expect(formatRtt(-1)).toEqual({ value: "—", unit: "" });
    expect(formatRtt(Number.NaN)).toEqual({ value: "—", unit: "" });
    expect(formatRtt(Number.POSITIVE_INFINITY)).toEqual({ value: "—", unit: "" });
  });
});
