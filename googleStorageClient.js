import fs from "fs";
import {Storage} from "@google-cloud/storage";

export function createStorageClient() {
    const storageOptions = {};

    // const base64Creds = process.env.GOOGLE_APPLICATION_CREDENTIALS_BASE64 || null;
    const inlineCreds = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON || null;
    // const keyFilePath = process.env.GOOGLE_APPLICATION_CREDENTIALS || null;

    const trySetCredentials = (raw, label) => {
        try {
            const parsed = JSON.parse(raw);
            if (parsed && typeof parsed === "object") {
                storageOptions.credentials = parsed;
                console.log(`[GCS] Using ${label} credentials from environment`);
                return true;
            }
        } catch (err) {
            console.error(`[GCS] Failed to parse ${label} credentials`, err);
        }
        return false;
    };

    // if (base64Creds) {
    //     const decoded = Buffer.from(base64Creds, "base64").toString("utf8");
    //     if (!trySetCredentials(decoded, "base64")) {
    //         console.error("Invalid GOOGLE_APPLICATION_CREDENTIALS_BASE64 value");
    //     }
    // }

    if (inlineCreds) {
        if (!trySetCredentials(inlineCreds, "inline")) {
            console.error("Invalid GOOGLE_APPLICATION_CREDENTIALS_JSON value");
        }
    }

    // if (keyFilePath) {
    //     if (fs.existsSync(keyFilePath)) {
    //         storageOptions.keyFilename = keyFilePath;
    //         console.log(`[GCS] Using key file credentials from ${keyFilePath}`);
    //     } else {
    //         console.warn(`[GCS] key file ${keyFilePath} not found; falling back to default credentials.`);
    //         delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
    //     }
    // }

    return new Storage(storageOptions);
}

