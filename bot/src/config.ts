import "dotenv/config";

type AppConfig = {
  botToken: string;
  databaseUrl: string;
  adminIds: Set<number>;
  timezone: string;
  workStart: string;
  workEnd: string;
  requestCooldownMinutes: number;
  maxRequestCount: number;
};

const requireEnv = (name: string): string => {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Environment variable ${name} is required`);
  }
  return value;
};

const parsePositiveInt = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
};

const parseAdminIds = (): Set<number> => {
  const raw = process.env.ADMIN_IDS?.trim();
  if (!raw) {
    return new Set<number>();
  }

  const ids = raw
    .split(",")
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => Number.parseInt(chunk, 10))
    .filter((value) => Number.isFinite(value));

  return new Set<number>(ids);
};

export const config: AppConfig = {
  botToken: requireEnv("MAX_BOT_TOKEN"),
  databaseUrl: requireEnv("DATABASE_URL"),
  adminIds: parseAdminIds(),
  timezone: process.env.BOT_TIMEZONE?.trim() || "Europe/Moscow",
  workStart: process.env.WORK_START?.trim() || "00:01",
  workEnd: process.env.WORK_END?.trim() || "20:59",
  requestCooldownMinutes: parsePositiveInt("REQUEST_COOLDOWN_MINUTES", 30),
  maxRequestCount: parsePositiveInt("MAX_REQUEST_COUNT", 5000),
};

export const LEGACY_INITIAL_USER_TIME = 1672520400;