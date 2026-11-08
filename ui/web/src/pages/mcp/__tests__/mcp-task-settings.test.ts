import { describe, expect, it } from "vitest";
import {
  EMPTY_TASK_SETTINGS,
  applyTaskSettings,
  hasTaskEvents,
  readTaskSettings,
  validateTaskSettings,
  type McpTaskSettingsForm,
} from "../mcp-task-settings";

const form = (patch: Partial<McpTaskSettingsForm>): McpTaskSettingsForm => ({ ...EMPTY_TASK_SETTINGS, ...patch });

describe("readTaskSettings", () => {
  it("gives empty values for missing or bad settings", () => {
    expect(readTaskSettings(undefined)).toEqual(EMPTY_TASK_SETTINGS);
    expect(readTaskSettings({ protocol: 5, task_events: "x" })).toEqual(EMPTY_TASK_SETTINGS);
  });

  it("reads protocol and task events", () => {
    const got = readTaskSettings({
      protocol: " Legacy ",
      task_events: { event: "workflow_run.status_changed", task_argument: "run_id", bootstrap_arguments: { workflow: "w" } },
    });
    expect(got.protocol).toBe("legacy");
    expect(got.event).toBe("workflow_run.status_changed");
    expect(got.taskArgument).toBe("run_id");
    expect(JSON.parse(got.bootstrapArguments)).toEqual({ workflow: "w" });
    expect(hasTaskEvents(got)).toBe(true);
  });
});

describe("validateTaskSettings", () => {
  it("accepts all fields empty", () => {
    expect(validateTaskSettings(EMPTY_TASK_SETTINGS)).toBeNull();
  });

  it("accepts a full binding", () => {
    expect(validateTaskSettings(form({ event: "e", taskArgument: "run_id", bootstrapArguments: '{"a":1}' }))).toBeNull();
  });

  it("needs both event and task argument", () => {
    expect(validateTaskSettings(form({ event: "e" }))?.key).toBe("form.errors.taskEventsIncomplete");
    expect(validateTaskSettings(form({ bootstrapArguments: "{}" }))?.key).toBe("form.errors.taskEventsIncomplete");
  });

  it("checks lengths in characters", () => {
    expect(validateTaskSettings(form({ event: "é".repeat(128), taskArgument: "a".repeat(64) }))).toBeNull();
    expect(validateTaskSettings(form({ event: "a".repeat(129), taskArgument: "x" }))?.key).toBe("form.errors.taskEventTooLong");
    expect(validateTaskSettings(form({ event: "e", taskArgument: "a".repeat(65) }))?.key).toBe("form.errors.taskArgumentTooLong");
  });

  it("needs bootstrap arguments to be a small JSON object", () => {
    const base = { event: "e", taskArgument: "run_id" };
    for (const text of ["not json", "[]", "null", '"x"', "1"]) {
      expect(validateTaskSettings(form({ ...base, bootstrapArguments: text }))?.key).toBe("form.errors.bootstrapInvalid");
    }
    const pad = "a".repeat(1024 - '{"k":""}'.length);
    expect(validateTaskSettings(form({ ...base, bootstrapArguments: `{ "k" : "${pad}" }` }))).toBeNull();
    expect(validateTaskSettings(form({ ...base, bootstrapArguments: `{"k":"${pad}a"}` }))?.key).toBe("form.errors.bootstrapTooLarge");
  });
});

describe("applyTaskSettings", () => {
  it("keeps other settings keys and hidden task_events keys", () => {
    const settings = {
      tool_hints: { global: "hint" },
      custom: 1,
      task_events: { event: "old", task_argument: "old", heartbeat_sec: 30, safety_poll_sec: 600 },
    };
    const got = applyTaskSettings(settings, form({ protocol: "legacy", event: " e ", taskArgument: "run_id", bootstrapArguments: '{"workflow":"w"}' }));
    expect(got).toEqual({
      tool_hints: { global: "hint" },
      custom: 1,
      protocol: "legacy",
      task_events: { event: "e", task_argument: "run_id", bootstrap_arguments: { workflow: "w" }, heartbeat_sec: 30, safety_poll_sec: 600 },
    });
    expect(settings.task_events.event).toBe("old");
  });

  it("removes protocol for auto and task_events when empty", () => {
    const got = applyTaskSettings(
      { protocol: "legacy", task_events: { event: "e", task_argument: "a", bootstrap_arguments: { x: 1 } }, custom: true },
      EMPTY_TASK_SETTINGS,
    );
    expect(got).toEqual({ custom: true });
  });

  it("removes bootstrap arguments when the text is cleared", () => {
    const got = applyTaskSettings(
      { task_events: { event: "e", task_argument: "a", bootstrap_arguments: { x: 1 } } },
      form({ event: "e", taskArgument: "a" }),
    );
    expect(got).toEqual({ task_events: { event: "e", task_argument: "a" } });
  });
});
