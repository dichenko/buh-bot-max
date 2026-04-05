import { Pool } from "pg";
import { config } from "./config";

export const pool = new Pool({
  connectionString: config.databaseUrl,
});

export const closeDb = async (): Promise<void> => {
  await pool.end();
};