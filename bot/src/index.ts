import { Bot } from "@maxhub/max-bot-api";
import { config } from "./config";
import { closeDb } from "./db";
import { registerHandlers } from "./handlers/registerHandlers";

const bot = new Bot(config.botToken);

bot.api.setMyCommands([
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
]);

registerHandlers(bot);

bot.catch((error) => {
  console.error("Unhandled bot error:", error);
  process.exit(1);
});

const shutdown = async (signal: string): Promise<void> => {
  console.log(`Received ${signal}, shutting down...`);
  await closeDb();
  process.exit(0);
};

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});
process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});

bot.start();
console.log("MAX bot started");
