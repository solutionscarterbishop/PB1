import dotenv from "dotenv";
dotenv.config();

console.log("CWD:", process.cwd());
console.log("SUPABASE_URL:", process.env.SUPABASE_URL);
console.log("HAS_SERVICE_KEY:", !!process.env.SUPABASE_SERVICE_ROLE_KEY);
