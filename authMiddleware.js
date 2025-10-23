import "./env.js";

import { jwtVerify } from "jose";
import cookie from "cookie";
import { createServerClient } from "@supabase/ssr";

function createSupabaseClient(req, res) {
    return createServerClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_ANON_KEY,
        {
            cookies: {
                get(name) {
                    const cookies = cookie.parse(req.headers.cookie || "");
                    return cookies[name];
                },
                set() {},
                remove() {},
            },
        }
    );
}

const secret = new TextEncoder().encode(process.env.SUPABASE_JWT_SECRET);

export async function verifyAuth(req, res, next) {
    if(req.path === '/readiness_check' || req.path === '/liveness_check') {
        return next();
    }

    console.log('request to path: ' + req.path)

    try {
        // 1️⃣ Get token from Authorization header
        const authHeader = req.headers.authorization;
        let token = null;

        if (authHeader?.startsWith("Bearer ")) {
            token = authHeader.split(" ")[1].trim();
        }

        // 2️⃣ If no Authorization header, try cookies (Supabase session)
        if (!token) {
            const supabase = createSupabaseClient(req, res);
            const { data: { session }, error } = await supabase.auth.getSession();

            if (error) throw new Error("Supabase session fetch failed: " + error.message);
            token = session?.access_token;
        }

        // 3️⃣ Still no token? Unauthorized
        if (!token) {
            return res.status(401).json({ error: "Missing or invalid JWT" });
        }

        // 4️⃣ Verify token
        const { payload } = await jwtVerify(token, secret);

        // 5️⃣ Attach user payload to request
        req.user = payload;

        return next();
    } catch (err) {
        console.error("Auth error:", err);
        return res.status(401).json({ error: "Unauthorized: " + err.message });
    }
}

export async function getUserFromToken(token) {
    try {
        const { payload } = await jwtVerify(token, secret);
        return {
            id: payload.sub,
            email: payload.email,
            payload
        };
    } catch (err) {
        throw new Error("Invalid token: " + err.message);
    }
}
