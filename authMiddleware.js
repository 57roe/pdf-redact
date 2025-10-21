import { jwtVerify } from "jose";
import dotenv from "dotenv";
dotenv.config();

const secret = new TextEncoder().encode(process.env.SUPABASE_JWT_SECRET);

export async function verifyAuth(req, res, next) {
    try {
        const authHeader = req.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ error: 'Missing or invalid Authorization header' });
        }
        console.log("🔑 Received header:", req.headers.authorization);

        const token = authHeader.split(' ')[1];
        if (!token || token === 'token') {
            return res.status(401).json({ error: 'Invalid or missing JWT' });
        }

        const { payload } = await jwtVerify(token, secret);
        req.user = payload;

        next();
    } catch (err) {
        console.error('Auth error:', err);
        return res.status(401).json({ error: 'Unauthorized' });
    }
}
