import { Bot, Context } from "@maxhub/max-bot-api";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { config } from "./config";
import { closeDb } from "./db";
import { registerHandlers } from "./handlers/registerHandlers";

const bot = new Bot(config.botToken);

const MAX_BODY_SIZE_BYTES = 1024 * 1024;

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

const toHttpError = (message: string, statusCode: number): HttpError => {
  const error = new Error(message) as HttpError;
  error.statusCode = statusCode;
  return error;
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

const handleWebhookRequest = async (
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> => {
  const requestSecret = req.headers["x-max-bot-api-secret"];
  const normalizedSecret = Array.isArray(requestSecret) ? requestSecret[0] : requestSecret;
  if (config.webhookSecret && normalizedSecret !== config.webhookSecret) {
    console.warn(
      `Webhook rejected: invalid secret for ${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,
    );
    sendJson(res, 401, { ok: false, message: "Invalid webhook secret" });
    return;
  }

  const rawBody = await readBody(req);
  const update = parseUpdate(rawBody);
  await dispatchUpdate(update);

  const updateType =
    typeof (update as { update_type?: unknown }).update_type === "string"
      ? (update as { update_type: string }).update_type
      : "unknown";
  console.log(`Webhook processed: ${updateType}`);

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
      console.warn(`Webhook rejected: invalid method ${req.method ?? "UNKNOWN"}`);
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
      console.error("Unhandled webhook error:", error);
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
  console.log(`Received ${signal}, shutting down...`);
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

  console.log(
    `MAX bot webhook server started on port ${config.webhookPort}, path ${config.webhookPath}`,
  );
  if (config.webhookUrl) {
    console.log(`Expected webhook URL: ${config.webhookUrl}`);
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
        console.log("MAX bot commands synced and bot info loaded.");
        return;
      } catch (error) {
        const delayMs = Math.min(30000, attempt * 5000);
        console.warn(
          `MAX API startup sync failed (attempt ${attempt}), retry in ${Math.floor(delayMs / 1000)}s:`,
          error,
        );
        await sleep(delayMs);
      }
    }
  })();
};

void start().catch(async (error) => {
  console.error("Failed to start MAX bot webhook server:", error);
  await closeDb();
  process.exit(1);
});
