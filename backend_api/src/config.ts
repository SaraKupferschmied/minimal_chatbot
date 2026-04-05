import dotenv from "dotenv";
import path from "node:path";

const envPath = path.resolve(process.cwd(), "../.env");
const result = dotenv.config({ path: envPath });

console.log("ENV PATH:", envPath);
console.log("DOTENV PARSED:", result.parsed);
console.log("DOTENV ERROR:", result.error?.message);