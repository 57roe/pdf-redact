import "./env.js";

import {createClient} from "@supabase/supabase-js";
import {createServerClient} from "@supabase/ssr";

let adminClient = null;

export function createAdminClient() {
    const url = process.env.SUPABASE_URL;
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url) {
        throw new Error("SUPABASE_URL is not set");
    }

    if (!serviceRoleKey) {
        throw new Error("SUPABASE_SERVICE_ROLE_KEY is not set");
    }

    if (!adminClient) {
        adminClient = createClient(url, serviceRoleKey, {
            auth: {
                persistSession: false,
                autoRefreshToken: false,
            },
            global: {
                headers: {
                    "X-Client-Info": "bankstatement2csv-admin",
                },
            },
        });
    }

    return adminClient;
}

export function createSupabaseClient(req, res) {
    const supabase = createClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_ANON_KEY,
        {global: {fetch}}
    );

    // ✅ Extract user token if present
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;

    if (token) {
        supabase.auth.setSession({access_token: token, refresh_token: token});
    }

    return supabase;
}

export async function getTokenFromSupabaseClient(req, res) {
    console.log('Environment variables: ', process.env.SUPABASE_URL)

    const supabase = createClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_ANON_KEY,
        {global: {fetch}}
    );

    const {
        data: {session},
        error
    } = await supabase.auth.getSession();
    console.log(session)

    if (error) throw error;

    return session?.access_token;
}



