import { describe, it, expect } from "vitest";
import { PROVIDER_TYPES } from "@/constants/providers";

describe("PROVIDER_TYPES", () => {
  it("includes a custom entry for generic OpenAI-compatible endpoints", () => {
    const custom = PROVIDER_TYPES.find((pt) => pt.value === "custom");
    expect(custom).toBeDefined();
    expect(custom?.label).toBe("Custom Provider");
    expect(custom?.apiBase).toBe("");
    expect(custom?.placeholder).not.toBe("");
  });

  it("has unique `value` across all entries", () => {
    const values = PROVIDER_TYPES.map((pt) => pt.value);
    expect(new Set(values).size).toBe(values.length);
  });
});
