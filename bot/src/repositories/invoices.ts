import { pool } from "../db";

type CreateInvoiceInput = {
  orgId: number;
  orgName: string;
  orgInn: string;
  orgCountPrefix: string;
  orgPrice: number;
  maxUserId: number;
  count: number;
};

type InvoiceResult = {
  id: number;
  number: number;
  date: string;
};

export const createInvoiceIp = async (payload: CreateInvoiceInput): Promise<InvoiceResult> => {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const nextNumberRes = await client.query<{ next_number: number }>(
      "SELECT COALESCE(MAX(number), 0) + 1 AS next_number FROM invoices_ip",
    );
    const invoiceNumber = Number(nextNumberRes.rows[0]?.next_number ?? 1);

    const insertRes = await client.query<InvoiceResult>(
      `
        INSERT INTO invoices_ip (
          number,
          org_id,
          org_name,
          org_inn,
          org_count,
          org_price,
          date,
          user_id,
          count
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8)
        RETURNING id, number, date
      `,
      [
        invoiceNumber,
        payload.orgId,
        payload.orgName,
        payload.orgInn,
        payload.orgCountPrefix,
        payload.orgPrice,
        payload.maxUserId,
        payload.count,
      ],
    );

    await client.query("COMMIT");

    const row = insertRes.rows[0];
    if (!row) {
      throw new Error("Failed to create invoice");
    }

    return row;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};