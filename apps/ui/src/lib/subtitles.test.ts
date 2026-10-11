import { describe, expect, it } from "vitest";
import { formatStamp, parseCues } from "./subtitles";

describe("parseCues", () => {
  it("reads SRT", () => {
    const srt = "1\n00:00:00,000 --> 00:00:02,800\nHello there.\n\n2\n00:00:03,000 --> 00:01:05,500\nSecond\nline.\n";
    expect(parseCues(srt)).toEqual([
      { start: 0, end: 2.8, text: "Hello there." },
      { start: 3, end: 65.5, text: "Second\nline." },
    ]);
  });

  it("reads WebVTT, skipping the header, notes and cue settings", () => {
    const vtt = "WEBVTT\n\nNOTE a comment\n\nintro\n00:03.000 --> 00:05.000 align:start\nHi.\n";
    expect(parseCues(vtt)).toEqual([{ start: 3, end: 5, text: "Hi." }]);
  });

  it("finds nothing in plain text", () => {
    expect(parseCues("Just a sentence.")).toEqual([]);
  });

  it("copes with Windows line endings", () => {
    expect(parseCues("1\r\n00:00:01,000 --> 00:00:02,000\r\nOk\r\n")).toHaveLength(1);
  });
});

describe("formatStamp", () => {
  it("shows minutes and seconds, adding hours when needed", () => {
    expect(formatStamp(0)).toBe("0:00");
    expect(formatStamp(75.4)).toBe("1:15");
    expect(formatStamp(3723)).toBe("1:02:03");
  });
});
