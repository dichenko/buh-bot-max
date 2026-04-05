import { pool } from "../db";
import { LEGACY_INITIAL_USER_TIME } from "../config";

export type UserRow = {
  id: number;
  user_id: number | null;
  max_user_id: number | null;
  org_id: number;
  user_time: number;
};

export const getUserByMaxUserId = async (maxUserId: number): Promise<UserRow | null> => {
  const result = await pool.query<UserRow>(
    `
      SELECT id, user_id, max_user_id, org_id, user_time
      FROM users
      WHERE max_user_id = $1
      ORDER BY id ASC
      LIMIT 1
    `,
    [maxUserId],
  );

  return result.rows[0] ?? null;
};

export const addUserWithMaxId = async (maxUserId: number, orgId: number): Promise<UserRow | null> => {
  const existing = await getUserByMaxUserId(maxUserId);
  if (existing) {
    return existing;
  }

  const result = await pool.query<UserRow>(
    `
      INSERT INTO users (user_id, max_user_id, org_id, user_time)
      VALUES ($1, $2, $3, $4)
      RETURNING id, user_id, max_user_id, org_id, user_time
    `,
    [null, maxUserId, orgId, LEGACY_INITIAL_USER_TIME],
  );

  return result.rows[0] ?? null;
};

export const bindMaxIdToLegacyUser = async (
  legacyUserId: number,
  maxUserId: number,
): Promise<UserRow | null> => {
  const result = await pool.query<UserRow>(
    `
      UPDATE users
      SET max_user_id = $2
      WHERE user_id = $1
      RETURNING id, user_id, max_user_id, org_id, user_time
    `,
    [legacyUserId, maxUserId],
  );

  return result.rows[0] ?? null;
};

export const deleteUserByMaxId = async (maxUserId: number): Promise<boolean> => {
  const result = await pool.query(
    `
      DELETE FROM users
      WHERE max_user_id = $1
    `,
    [maxUserId],
  );

  return (result.rowCount ?? 0) > 0;
};

export const updateUserTimeById = async (id: number, unixTime: number): Promise<void> => {
  await pool.query(
    `
      UPDATE users
      SET user_time = $2
      WHERE id = $1
    `,
    [id, unixTime],
  );
};