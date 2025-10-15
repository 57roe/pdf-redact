import { jwtVerify } from "jose";
import dotenv from "dotenv";
dotenv.config();

const secret = new TextEncoder().encode(process.env.SUPABASE_JWT_SECRET);

export async function verifyAuth(req, res, next) {
    try {
        const authHeader = req.headers.authorization;
        if (!authHeader?.startsWith("Bearer ")) {
            return res.status(401).json({ error: "Missing or invalid Authorization header" });
        }

        const token = authHeader.split(" ")[1];

        const { payload } = await jwtVerify(token, secret, {
            algorithms: ["HS256"],
        });

        req.user = payload;
        next();
    } catch (err) {
        console.error("Auth error:", err);
        res.status(401).json({ error: "Invalid or expired token" });
    }
}
