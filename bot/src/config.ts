import "dotenv/config";

type AppConfig = {
  botToken: string;
  botSubdomain: string;
  databaseUrl: string;
  adminIds: Set<number>;
  adminEmails: string[];
  timezone: string;
  workStart: string;
  workEnd: string;
  requestCooldownMinutes: number;
  maxRequestCount: number;
  megaplan: {
    token: string;
    url: string;
  };
  smtp: {
    host: string;
    port: number | null;
    secure: boolean;
    user: string;
    password: string;
    from: string;
  };
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

const parseOptionalInt = (name: string): number | null => {
  const raw = process.env[name]?.trim();
  if (!raw) {
    return null;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer when provided`);
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

const parseAdminEmails = (): string[] => {
  const raw = process.env.ADMIN_EMAIL?.trim();
  if (!raw) {
    return [];
  }

  return raw
    .split(",")
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((value) => value.toLowerCase());
};

export const config: AppConfig = {
  botToken: requireEnv("MAX_BOT_TOKEN"),
  botSubdomain: process.env.BOT_SUBDOMAIN?.trim() || "",
  databaseUrl: requireEnv("DATABASE_URL"),
  adminIds: parseAdminIds(),
  adminEmails: parseAdminEmails(),
  timezone: process.env.BOT_TIMEZONE?.trim() || "Europe/Moscow",
  workStart: process.env.WORK_START?.trim() || "00:01",
  workEnd: process.env.WORK_END?.trim() || "20:59",
  requestCooldownMinutes: parsePositiveInt("REQUEST_COOLDOWN_MINUTES", 30),
  maxRequestCount: parsePositiveInt("MAX_REQUEST_COUNT", 5000),
  megaplan: {
    token: process.env.TOKEN_MEGAPLAN?.trim() || "",
    url: process.env.URL_MEGAPLAN?.trim() || "",
  },
  smtp: {
    host: process.env.SMTP_HOST?.trim() || "",
    port: parseOptionalInt("SMTP_PORT"),
    secure: (process.env.SMTP_SECURE?.trim().toLowerCase() || "false") === "true",
    user: process.env.SMTP_USER?.trim() || "",
    password: process.env.SMTP_PASSWORD?.trim() || "",
    from: process.env.SMTP_FROM?.trim() || "",
  },
};

export const LEGACY_INITIAL_USER_TIME = 1672520400;
