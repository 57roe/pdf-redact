if (process.env.NODE_ENV !== "production") {
    const dotenv = await import("dotenv");
    console.log("[env.js] Loading environment variables...");
    dotenv.config();
    console.log("[env.js] SUPABASE_URL:", process.env.SUPABASE_URL ? "✓ loaded" : "✗ missing");
}


