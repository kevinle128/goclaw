/**
 * Form state and helpers for the MCP server settings "protocol" and
 * "task_events". The gateway checks the same limits on create and update
 * (internal/mcp/settings_validate.go); the check here only gives an earlier,
 * clearer error.
 */

export type McpProtocolMode = "auto" | "legacy";

export const MCP_PROTOCOL_MODES: readonly McpProtocolMode[] = ["auto", "legacy"];

export const TASK_EVENT_MAX_CHARS = 128;
export const TASK_ARGUMENT_MAX_CHARS = 64;
export const BOOTSTRAP_MAX_BYTES = 1024;

export interface McpTaskSettingsForm {
  protocol: McpProtocolMode;
  event: string;
  taskArgument: string;
  /** bootstrap_arguments as JSON text; empty means not set. */
  bootstrapArguments: string;
}

export const EMPTY_TASK_SETTINGS: McpTaskSettingsForm = {
  protocol: "auto",
  event: "",
  taskArgument: "",
  bootstrapArguments: "",
};

/** i18n key (in the "mcp" namespace) and values of a validation error. */
export interface McpTaskSettingsError {
  key: string;
  values?: Record<string, number>;
}

type JsonObject = Record<string, unknown>;

function isPlainObject(v: unknown): v is JsonObject {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

const charCount = (s: string) => [...s].length;

/** Reads the form state from a server's settings object. */
export function readTaskSettings(settings: unknown): McpTaskSettingsForm {
  if (!isPlainObject(settings)) return { ...EMPTY_TASK_SETTINGS };
  const rawProtocol = typeof settings.protocol === "string" ? settings.protocol.trim().toLowerCase() : "";
  const protocol: McpProtocolMode = rawProtocol === "legacy" ? "legacy" : "auto";
  const te = isPlainObject(settings.task_events) ? settings.task_events : {};
  const bootstrap = te.bootstrap_arguments;
  return {
    protocol,
    event: typeof te.event === "string" ? te.event : "",
    taskArgument: typeof te.task_argument === "string" ? te.task_argument : "",
    bootstrapArguments: isPlainObject(bootstrap) ? JSON.stringify(bootstrap, null, 2) : "",
  };
}

/** Reports whether any task events field has a value. */
export function hasTaskEvents(form: McpTaskSettingsForm): boolean {
  return form.event.trim() !== "" || form.taskArgument.trim() !== "" || form.bootstrapArguments.trim() !== "";
}

/** Parses bootstrap_arguments text. Empty text gives undefined. */
function parseBootstrap(text: string): { value?: JsonObject; error?: McpTaskSettingsError } {
  if (text.trim() === "") return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { error: { key: "form.errors.bootstrapInvalid" } };
  }
  if (!isPlainObject(parsed)) return { error: { key: "form.errors.bootstrapInvalid" } };
  if (new TextEncoder().encode(JSON.stringify(parsed)).length > BOOTSTRAP_MAX_BYTES) {
    return { error: { key: "form.errors.bootstrapTooLarge", values: { max: BOOTSTRAP_MAX_BYTES } } };
  }
  return { value: parsed };
}

/** Returns the error of the bootstrap_arguments text, or null. Empty text is valid. */
export function validateBootstrapArguments(text: string): McpTaskSettingsError | null {
  return parseBootstrap(text).error ?? null;
}

/** Returns the first error of the task events fields, or null. All fields empty is valid. */
export function validateTaskSettings(form: McpTaskSettingsForm): McpTaskSettingsError | null {
  if (!hasTaskEvents(form)) return null;
  const event = form.event.trim();
  const arg = form.taskArgument.trim();
  if (event === "" || arg === "") return { key: "form.errors.taskEventsIncomplete" };
  if (charCount(event) > TASK_EVENT_MAX_CHARS) {
    return { key: "form.errors.taskEventTooLong", values: { max: TASK_EVENT_MAX_CHARS } };
  }
  if (charCount(arg) > TASK_ARGUMENT_MAX_CHARS) {
    return { key: "form.errors.taskArgumentTooLong", values: { max: TASK_ARGUMENT_MAX_CHARS } };
  }
  return validateBootstrapArguments(form.bootstrapArguments);
}

/**
 * Writes the form state into a copy of settings. Other settings keys stay.
 * "auto" protocol and empty task events remove their keys. Keys of the
 * existing task_events that the form does not show (heartbeat_sec,
 * safety_poll_sec) stay. Call validateTaskSettings first.
 */
export function applyTaskSettings(settings: JsonObject, form: McpTaskSettingsForm): JsonObject {
  const next: JsonObject = { ...settings };
  if (form.protocol === "legacy") next.protocol = "legacy";
  else delete next.protocol;

  if (!hasTaskEvents(form)) {
    delete next.task_events;
    return next;
  }
  const te: JsonObject = isPlainObject(settings.task_events) ? { ...settings.task_events } : {};
  te.event = form.event.trim();
  te.task_argument = form.taskArgument.trim();
  const bootstrap = parseBootstrap(form.bootstrapArguments).value;
  if (bootstrap) te.bootstrap_arguments = bootstrap;
  else delete te.bootstrap_arguments;
  next.task_events = te;
  return next;
}
