import { Pool, type QueryResultRow } from "pg";

const poolConfig = {
  host: process.env.POSTGRES_HOST || "localhost",
  port: Number(process.env.POSTGRES_PORT || 5432),
  database: process.env.POSTGRES_DB || "",
  user: process.env.POSTGRES_USER || "",
  password: process.env.POSTGRES_PASSWORD || "",
};

console.log("DB CONFIG CHECK:", {
  host: poolConfig.host,
  port: poolConfig.port,
  database: poolConfig.database,
  user: poolConfig.user,
  passwordType: typeof poolConfig.password,
});

export const pool = new Pool(poolConfig);

export async function query<T extends QueryResultRow>(
  text: string,
  params: unknown[] = []
): Promise<T[]> {
  const res = await pool.query<T>(text, params);
  return res.rows;
}