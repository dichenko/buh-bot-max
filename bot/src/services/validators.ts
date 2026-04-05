const parseTimeToMinutes = (value: string): number => {
  const parts = value.split(":");
  if (parts.length !== 2) {
    throw new Error(`Invalid time format: ${value}`);
  }

  const hours = Number.parseInt(parts[0] ?? "", 10);
  const minutes = Number.parseInt(parts[1] ?? "", 10);
  if (
    !Number.isFinite(hours) ||
    !Number.isFinite(minutes) ||
    hours < 0 ||
    hours > 23 ||
    minutes < 0 ||
    minutes > 59
  ) {
    throw new Error(`Invalid time value: ${value}`);
  }

  return hours * 60 + minutes;
};

const getTimeInMinutes = (date: Date, timeZone: string): number => {
  const formatted = new Intl.DateTimeFormat("en-GB", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);

  return parseTimeToMinutes(formatted);
};

export const isWithinWorkingHours = (
  date: Date,
  timeZone: string,
  start: string,
  end: string,
): { ok: boolean; currentTime: string } => {
  const nowFormatter = new Intl.DateTimeFormat("ru-RU", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  const currentTime = nowFormatter.format(date);
  const nowMinutes = parseTimeToMinutes(
    new Intl.DateTimeFormat("en-GB", {
      timeZone,
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(date),
  );

  const startMinutes = parseTimeToMinutes(start);
  const endMinutes = parseTimeToMinutes(end);

  if (startMinutes <= endMinutes) {
    return {
      ok: nowMinutes >= startMinutes && nowMinutes <= endMinutes,
      currentTime,
    };
  }

  return {
    ok: nowMinutes >= startMinutes || nowMinutes <= endMinutes,
    currentTime,
  };
};

export const parseCount = (rawText: string, maxAllowed: number): number | null => {
  const value = rawText.trim();
  if (!/^\d+$/.test(value)) {
    return null;
  }

  const count = Number.parseInt(value, 10);
  if (!Number.isFinite(count) || count <= 0 || count > maxAllowed) {
    return null;
  }

  return count;
};

export const remainingCooldownMinutes = (
  lastUnixTime: number,
  nowUnixTime: number,
  cooldownMinutes: number,
): number => {
  const elapsed = nowUnixTime - lastUnixTime;
  const cooldownSec = cooldownMinutes * 60;

  if (elapsed >= cooldownSec) {
    return 0;
  }

  return Math.ceil((cooldownSec - elapsed) / 60);
};

export const getNowInTimeZone = (timeZone: string): Date => {
  const now = new Date();
  const localString = now.toLocaleString("en-US", { timeZone });
  return new Date(localString);
};

export const formatPrice = (value: number): string => {
  return new Intl.NumberFormat("ru-RU").format(value);
};