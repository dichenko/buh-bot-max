import { config } from "../config";

const timestampFormatter = new Intl.DateTimeFormat("sv-SE", {
  timeZone: config.timezone,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

const timestamp = (): string => {
  return timestampFormatter.format(new Date()).replace(" ", "T");
};

const write = (
  method: "log" | "warn" | "error",
  level: "INFO" | "WARN" | "ERROR",
  message: string,
  ...args: unknown[]
): void => {
  console[method](`[${timestamp()} ${config.timezone}] [${level}] ${message}`, ...args);
};

export const logger = {
  info(message: string, ...args: unknown[]): void {
    write("log", "INFO", message, ...args);
  },
  warn(message: string, ...args: unknown[]): void {
    write("warn", "WARN", message, ...args);
  },
  error(message: string, ...args: unknown[]): void {
    write("error", "ERROR", message, ...args);
  },
};
