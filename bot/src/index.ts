import { Bot, Context } from "@maxhub/max-bot-api";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { config } from "./config";
import { closeDb } from "./db";
import { registerHandlers } from "./handlers/registerHandlers";
import { logger } from "./services/logger";

const bot = new Bot(config.botToken);

const MAX_BODY_SIZE_BYTES = 1024 * 1024;
const PROCESSED_UPDATE_TTL_MS = 30 * 60 * 1000;
const PROCESSED_UPDATE_CACHE_LIMIT = 10_000;

const processedUpdateKeys = new Map<string, number>();
const inFlightUpdates = new Map<string, Promise<void>>();

const botCommands = [
  {
    name: "start",
    description: "Начать работу с ботом",
  },
  {
    name: "ping",
    description: "Проверка доступности бота",
  },
  {
    name: "add",
    description: "[admin] /add <max_user_id> <org_id>",
  },
  {
    name: "del",
    description: "[admin] /del <max_user_id>",
  },
  {
    name: "bindmax",
    description: "[admin] /bindmax <legacy_tg_user_id> <max_user_id>",
  },
];

registerHandlers(bot);

let botInfo: unknown;

const sleep = (ms: number): Promise<void> => {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
};

type HttpError = Error & { statusCode: number };
type JsonObject = Record<string, unknown>;

const toHttpError = (message: string, statusCode: number): HttpError => {
  const error = new Error(message) as HttpError;
  error.statusCode = statusCode;
  return error;
};

const asObject = (value: unknown): JsonObject | null => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value as JsonObject;
};

const asString = (value: unknown): string | null => {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed ? trimmed : null;
};

const asNumber = (value: unknown): number | null => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }

  return value;
};

const getUpdateType = (update: unknown): string => {
  const payload = asObject(update);
  return asString(payload?.update_type) ?? "unknown";
};

const buildUpdateDedupKey = (update: unknown): string => {
  const payload = asObject(update);
  const updateType = getUpdateType(update);
  if (!payload) {
    return `${updateType}:raw`;
  }

  const directMessageId = asString(payload.message_id);
  if (directMessageId) {
    return `${updateType}:message_id:${directMessageId}`;
  }

  const message = asObject(payload.message);
  const messageBody = asObject(message?.body);
  const messageMid = asString(messageBody?.mid);
  if (messageMid) {
    return `${updateType}:mid:${messageMid}`;
  }

  const callback = asObject(payload.callback);
  const callbackId = asString(callback?.callback_id);
  if (callbackId) {
    return `${updateType}:callback_id:${callbackId}`;
  }

  const sessionId = asString(payload.session_id);
  if (sessionId) {
    return `${updateType}:session_id:${sessionId}`;
  }

  const timestamp = asNumber(payload.timestamp) ?? 0;
  const chatId =
    asNumber(payload.chat_id) ??
    asNumber(asObject(message?.recipient)?.chat_id) ??
    asNumber(asObject(payload.chat)?.chat_id) ??
    0;
  const userId =
    asNumber(asObject(payload.user)?.user_id) ??
    asNumber(asObject(message?.sender)?.user_id) ??
    asNumber(asObject(callback?.user)?.user_id) ??
    0;

  return `${updateType}:ts:${timestamp}:chat:${chatId}:user:${userId}`;
};

const pruneProcessedUpdates = (): void => {
  const nowMs = Date.now();
  for (const [key, expiresAt] of processedUpdateKeys) {
    if (expiresAt <= nowMs) {
      processedUpdateKeys.delete(key);
    }
  }
};

const trimProcessedUpdates = (): void => {
  while (processedUpdateKeys.size > PROCESSED_UPDATE_CACHE_LIMIT) {
    const oldestKey = processedUpdateKeys.keys().next().value;
    if (typeof oldestKey !== "string") {
      return;
    }

    processedUpdateKeys.delete(oldestKey);
  }
};

const rememberProcessedUpdate = (dedupKey: string): void => {
  processedUpdateKeys.set(dedupKey, Date.now() + PROCESSED_UPDATE_TTL_MS);
  trimProcessedUpdates();
};

const sendJson = (res: ServerResponse, statusCode: number, payload: unknown): void => {
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(payload));
};

const readBody = (req: IncomingMessage): Promise<string> => {
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;

    req.on("data", (chunk: Buffer) => {
      totalBytes += chunk.length;
      if (totalBytes > MAX_BODY_SIZE_BYTES) {
        reject(toHttpError("Payload too large", 413));
        req.destroy();
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      resolve(Buffer.concat(chunks).toString("utf8"));
    });

    req.on("error", () => {
      reject(toHttpError("Failed to read request body", 400));
    });
  });
};

const getPathname = (req: IncomingMessage): string => {
  try {
    const rawUrl = req.url ?? "/";
    return new URL(rawUrl, "http://localhost").pathname;
  } catch {
    return "/";
  }
};

const parseUpdate = (rawBody: string): unknown => {
  if (!rawBody.trim()) {
    throw toHttpError("Empty body", 400);
  }

  try {
    const payload = JSON.parse(rawBody) as unknown;
    if (!payload || typeof payload !== "object") {
      throw toHttpError("Invalid update payload", 400);
    }
    return payload;
  } catch (error) {
    if ((error as HttpError).statusCode) {
      throw error;
    }
    throw toHttpError("Invalid JSON", 400);
  }
};

const dispatchUpdate = async (update: unknown): Promise<void> => {
  const ctx = new Context(update as any, bot.api, botInfo as any);
  await bot.middleware()(ctx as any, () => Promise.resolve(undefined));
};

const dispatchUpdateDeduplicated = async (
  update: unknown,
): Promise<{ dedupKey: string; duplicate: boolean }> => {
  const dedupKey = buildUpdateDedupKey(update);
  pruneProcessedUpdates();

  const nowMs = Date.now();
  const expiresAt = processedUpdateKeys.get(dedupKey);
  if (typeof expiresAt === "number") {
    if (expiresAt > nowMs) {
      return { dedupKey, duplicate: true };
    }

    processedUpdateKeys.delete(dedupKey);
  }

  const existing = inFlightUpdates.get(dedupKey);
  if (existing) {
    await existing;
    return { dedupKey, duplicate: true };
  }

  const processing = dispatchUpdate(update);
  inFlightUpdates.set(dedupKey, processing);

  try {
    await processing;
    rememberProcessedUpdate(dedupKey);
    return { dedupKey, duplicate: false };
  } finally {
    inFlightUpdates.delete(dedupKey);
  }
};

const handleWebhookRequest = async (
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> => {
  const requestSecret = req.headers["x-max-bot-api-secret"];
  const normalizedSecret = Array.isArray(requestSecret) ? requestSecret[0] : requestSecret;
  if (config.webhookSecret && normalizedSecret !== config.webhookSecret) {
    logger.warn(
      `Webhook rejected: invalid secret for ${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,
    );
    sendJson(res, 401, { ok: false, message: "Invalid webhook secret" });
    return;
  }

  const rawBody = await readBody(req);
  const update = parseUpdate(rawBody);
  const updateType = getUpdateType(update);
  const { dedupKey, duplicate } = await dispatchUpdateDeduplicated(update);
  if (duplicate) {
    logger.warn(`Webhook duplicate skipped: ${updateType} (${dedupKey})`);
  } else {
    logger.info(`Webhook processed: ${updateType} (${dedupKey})`);
  }

  sendJson(res, 200, { ok: true });
};

const server = createServer((req, res) => {
  const pathname = getPathname(req);
  if (req.method === "GET" && pathname === "/healthz") {
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method !== "POST" || pathname !== config.webhookPath) {
    if (pathname === config.webhookPath) {
      logger.warn(`Webhook rejected: invalid method ${req.method ?? "UNKNOWN"}`);
    }

    sendJson(res, 404, { ok: false, message: "Not found" });
    return;
  }

  void handleWebhookRequest(req, res).catch((error: unknown) => {
    const typedError = error as Partial<HttpError>;
    const statusCode =
      typeof typedError.statusCode === "number" && typedError.statusCode >= 400
        ? typedError.statusCode
        : 500;

    if (statusCode >= 500) {
      logger.error("Unhandled webhook error:", error);
    }

    sendJson(res, statusCode, {
      ok: false,
      message: typedError.message ?? "Internal server error",
    });
  });
});

const closeServer = async (): Promise<void> => {
  if (!server.listening) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
};

const shutdown = async (signal: string): Promise<void> => {
  logger.info(`Received ${signal}, shutting down...`);
  await closeServer();
  await closeDb();
  process.exit(0);
};

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});
process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});

const start = async (): Promise<void> => {
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error): void => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = (): void => {
      server.off("error", onError);
      resolve();
    };

    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(config.webhookPort, () => {
      // no-op, waiting for "listening" event
    });
  });

  logger.info(
    `MAX bot webhook server started on port ${config.webhookPort}, path ${config.webhookPath}`,
  );
  if (config.webhookUrl) {
    logger.info(`Expected webhook URL: ${config.webhookUrl}`);
  }

  // MAX API may be temporarily unavailable (DNS/network). Keep webhook server
  // alive and retry metadata sync in background until it succeeds.
  void (async () => {
    let attempt = 0;
    while (true) {
      attempt += 1;
      try {
        await bot.api.setMyCommands(botCommands);
        botInfo = await bot.api.getMyInfo();
        logger.info("MAX bot commands synced and bot info loaded.");
        return;
      } catch (error) {
        const delayMs = Math.min(30000, attempt * 5000);
        logger.warn(
          `MAX API startup sync failed (attempt ${attempt}), retry in ${Math.floor(delayMs / 1000)}s:`,
          error,
        );
        await sleep(delayMs);
      }
    }
  })();
};

void start().catch(async (error) => {
  logger.error("Failed to start MAX bot webhook server:", error);
  await closeDb();
  process.exit(1);
});
