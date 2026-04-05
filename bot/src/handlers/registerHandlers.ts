import { Bot } from "@maxhub/max-bot-api";
import { config } from "../config";
import {
  addUserWithMaxId,
  bindMaxIdToLegacyUser,
  deleteUserByMaxId,
  getUserByMaxUserId,
  updateUserTimeById,
} from "../repositories/users";
import { getOrganizationByOrgId } from "../repositories/organizations";
import { createInvoiceIp } from "../repositories/invoices";
import {
  formatPrice,
  isWithinWorkingHours,
  parseCount,
  remainingCooldownMinutes,
} from "../services/validators";

const isAdmin = (id: number | undefined): boolean => {
  if (!id) {
    return false;
  }

  return config.adminIds.has(id);
};

const textFromMessage = (ctx: any): string => {
  const value = ctx?.message?.body?.text;
  return typeof value === "string" ? value.trim() : "";
};

const parsePositiveNumber = (raw: string): number | null => {
  if (!/^\d+$/.test(raw)) {
    return null;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
};

export const registerHandlers = (bot: Bot): void => {
  bot.command("start", async (ctx: any) => {
    const maxUserId = ctx.user?.user_id;
    if (!maxUserId) {
      return ctx.reply("Не удалось определить ваш MAX ID.");
    }

    const user = await getUserByMaxUserId(maxUserId);
    if (!user) {
      return ctx.reply(
        `Вы не зарегистрированы. Передайте администратору ваш MAX ID: ${maxUserId}`,
      );
    }

    const organization = await getOrganizationByOrgId(user.org_id);
    const orgName = organization?.org_name ?? `org_id=${user.org_id}`;

    return ctx.reply(
      `Здравствуйте. Организация: ${orgName}.\nОтправьте количество услуг числом (до ${config.maxRequestCount}).`,
    );
  });

  bot.command("ping", async (ctx: any) => {
    return ctx.reply("pong");
  });

  bot.command(/add\s+(\d+)\s+(\d+)/, async (ctx: any) => {
    const adminId = ctx.user?.user_id;
    if (!isAdmin(adminId)) {
      return ctx.reply("Недостаточно прав.");
    }

    const maxUserId = parsePositiveNumber(String(ctx.match?.[1] ?? ""));
    const orgId = parsePositiveNumber(String(ctx.match?.[2] ?? ""));

    if (!maxUserId || !orgId) {
      return ctx.reply("Формат: /add <max_user_id> <org_id>");
    }

    const user = await addUserWithMaxId(maxUserId, orgId);
    if (!user) {
      return ctx.reply("Не удалось добавить пользователя.");
    }

    return ctx.reply(`Пользователь добавлен: max_user_id=${maxUserId}, org_id=${orgId}`);
  });

  bot.command(/del\s+(\d+)/, async (ctx: any) => {
    const adminId = ctx.user?.user_id;
    if (!isAdmin(adminId)) {
      return ctx.reply("Недостаточно прав.");
    }

    const maxUserId = parsePositiveNumber(String(ctx.match?.[1] ?? ""));
    if (!maxUserId) {
      return ctx.reply("Формат: /del <max_user_id>");
    }

    const deleted = await deleteUserByMaxId(maxUserId);
    if (!deleted) {
      return ctx.reply("Пользователь не найден.");
    }

    return ctx.reply(`Пользователь max_user_id=${maxUserId} удален.`);
  });

  bot.command(/bindmax\s+(\d+)\s+(\d+)/, async (ctx: any) => {
    const adminId = ctx.user?.user_id;
    if (!isAdmin(adminId)) {
      return ctx.reply("Недостаточно прав.");
    }

    const legacyTgUserId = parsePositiveNumber(String(ctx.match?.[1] ?? ""));
    const maxUserId = parsePositiveNumber(String(ctx.match?.[2] ?? ""));

    if (!legacyTgUserId || !maxUserId) {
      return ctx.reply("Формат: /bindmax <legacy_tg_user_id> <max_user_id>");
    }

    const updated = await bindMaxIdToLegacyUser(legacyTgUserId, maxUserId);
    if (!updated) {
      return ctx.reply("Запись с таким legacy tg_user_id не найдена.");
    }

    return ctx.reply(
      `Привязка выполнена: legacy_tg_user_id=${legacyTgUserId} -> max_user_id=${maxUserId}`,
    );
  });

  bot.on("message_created", async (ctx: any) => {
    const messageText = textFromMessage(ctx);
    if (!messageText || messageText.startsWith("/")) {
      return;
    }

    const maxUserId = ctx.user?.user_id;
    if (!maxUserId) {
      return ctx.reply("Не удалось определить ваш MAX ID.");
    }

    const user = await getUserByMaxUserId(maxUserId);
    if (!user) {
      return ctx.reply(
        `Вы не зарегистрированы. Передайте администратору ваш MAX ID: ${maxUserId}`,
      );
    }

    const count = parseCount(messageText, config.maxRequestCount);
    if (!count) {
      return ctx.reply(
        `Неверный формат. Отправьте целое число от 1 до ${config.maxRequestCount}.`,
      );
    }

    const now = new Date();
    const workCheck = isWithinWorkingHours(now, config.timezone, config.workStart, config.workEnd);
    if (!workCheck.ok) {
      return ctx.reply(
        `Сейчас нерабочее время (${workCheck.currentTime}, ${config.timezone}). Прием заявок: ${config.workStart}-${config.workEnd}.`,
      );
    }

    const nowUnix = Math.floor(now.getTime() / 1000);
    const leftMinutes = remainingCooldownMinutes(
      user.user_time,
      nowUnix,
      config.requestCooldownMinutes,
    );
    if (leftMinutes > 0) {
      return ctx.reply(
        `Новая заявка будет доступна через ${leftMinutes} мин. Ограничение: 1 заявка в ${config.requestCooldownMinutes} мин.`,
      );
    }

    const organization = await getOrganizationByOrgId(user.org_id);
    if (!organization) {
      return ctx.reply(`Организация с org_id=${user.org_id} не найдена. Обратитесь к администратору.`);
    }

    const pricePerItem = Number(organization.org_price) + Number(organization.org_price_ip);
    const total = pricePerItem * count;

    const invoice = await createInvoiceIp({
      orgId: organization.org_id,
      orgName: organization.org_name,
      orgInn: organization.org_inn,
      orgCountPrefix: "ИП",
      orgPrice: pricePerItem,
      maxUserId,
      count,
    });

    await updateUserTimeById(user.id, nowUnix);

    return ctx.reply(
      [
        `Заявка принята.`,
        `Счет №${invoice.number}.`,
        `Организация: ${organization.org_name}.`,
        `Количество: ${count}.`,
        `Цена за 1 услугу: ${formatPrice(pricePerItem)} руб.`,
        `Сумма: ${formatPrice(total)} руб.`,
        "Далее заявку обработает Python-воркер документов.",
      ].join("\n"),
    );
  });
};
