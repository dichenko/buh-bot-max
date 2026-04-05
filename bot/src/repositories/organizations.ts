import { pool } from "../db";

export type OrganizationRow = {
  id: number;
  org_id: number;
  org_name: string;
  org_template: number;
  org_price: number;
  org_price_ip: number;
  org_inn: string;
  org_foundation: string | null;
  org_foundation_2: string | null;
};

export const getOrganizationByOrgId = async (orgId: number): Promise<OrganizationRow | null> => {
  const result = await pool.query<OrganizationRow>(
    `
      SELECT
        id,
        org_id,
        org_name,
        org_template,
        org_price,
        org_price_ip,
        org_inn,
        org_foundation,
        org_foundation_2
      FROM organizations
      WHERE org_id = $1
      ORDER BY id ASC
      LIMIT 1
    `,
    [orgId],
  );

  return result.rows[0] ?? null;
};