import "./env.js";

import express from "express";
import multer from "multer";
import { PDFDocument, rgb } from "pdf-lib";
import pkg from "pdfjs-dist/legacy/build/pdf.js";
import { v4 as uuidv4 } from "uuid";

const { getDocument } = pkg;
// import fs as fsPromises from "fs/promises";
import fs from "fs";
import path from "path";
import os from "os";
import { exec } from 'child_process';
import * as util from 'util';

import { verifyAuth, getUserFromToken } from "./authMiddleware.js";
import { createAdminClient, createSupabaseClient } from "./supabaseClients.js";

import { GoogleGenAI, createUserContent, createPartFromUri } from "@google/genai";
import { CloudTasksClient } from "@google-cloud/tasks";
import cors from "cors";

import { createStorageClient } from "./googleStorageClient.js";

const tasksClient = new CloudTasksClient();

const upload = multer();
const app = express();
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));
app.use(verifyAuth);


const googleAI = new GoogleGenAI({});
PDFDocument.embedStandardFont = async function patchedEmbed(fontName) {
    return PDFDocument.prototype.embedFont.call(this, fontName, {
        custom: true,
    });
};

// Provide a standard font data URL to silence Foxit font warnings.
// const fontsDir = path.join(process.cwd(), "fonts", "standard");
// if (fs.existsSync(fontsDir)) {
//     globalThis.standardFontDataUrl = fontsDir;
// }
const storage = createStorageClient();
const TEMP_BUCKET = 'pdf-temp-uploads';

app.use(cors({
    origin: ["https://bankstatement2csv.vercel.app", "https://bankstatement2csv.com", "https://www.bankstatement2csv.com"],
    methods: ["GET", "POST", "PUT", "DELETE"],
    // allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true
}));

// Convert exec to return a Promise
const execPromise = (command, args) => {
    return new Promise((resolve, reject) => {
        // Combine command and arguments safely
        const cmd = `${command} ${args.map(arg => `"${arg}"`).join(' ')}`;

        exec(cmd, (error, stdout, stderr) => {
            if (error) {
                // Include stderr in the error for better debugging
                error.message = `${error.message}\nstderr: ${stderr}`;
                return reject(error);
            }

            // Resolve with both stdout and stderr
            resolve({ stdout, stderr });
        });
    });
};

async function convertPdfToPng(inFile, tmpDir) {
    try {
        const result = await execPromise("pdftoppm", [
            "-png",    // Convert to PNG format
            "-r",      // Resolution flag
            "200",     // DPI resolution
            inFile,    // Input PDF file
            path.join(tmpDir, "page")  // Output path prefix
        ]);

        console.log('Conversion successful:');
        return result;
    } catch (error) {
        console.error('Error during PDF conversion:', error.message);
        throw error;
    }
}

/** blacklist (lowercase) to avoid false positives */
const NON_PII_TERMS = [
    "cont curent", "data detalii", "debit credit balanta", "cod fiscal platitor",
    "referinta", "iban", "swift", "sold initial", "sold final", "tranzactie",
    "plata", "transfer", "descriere", "balanta", "moneda", "data", "account",
    "current account", "transaction", "credit", "debit", "balance", "statement",
    "reference", "payment", "transfer", "currency", "details", "iban number",
    "swift code", "bank branch", "bank name", "realimentare", "alimentare",
    "cumparare", "home bank", "bank", "banca", "sucursala", "filiala",
    "nr. crt.", "nr.crt.", "nume", "prenume", "adresa", "telefon", "email",
    "tranzactie", "ing",
    "deposit", "withdrawal", "interest", "fee", "charges", "commission",
    "statement period", "monthly statement", "loan", "mortgage", "overdraft",
    "finance", "investment", "fund", "portfolio", "equity", "stock", "share",
    "bonds", "mutual fund", "savings", "checking", "ledger", "accounting",
    "invoice", "receipt", "tax", "vat", "payment date", "due date",
    "settlement", "clearing", "exchange", "foreign exchange", "fx", "rate",
    "interest rate", "banking", "financial", "capital", "market", "securities",
    "liability", "asset", "profit", "loss", "income", "expense", "budget",
    "cash flow", "audit", "report", "transaction id", "transaction type",
    "reconciliation", "bank statement", "beneficiary", "payer", "payee",
    "merchant", "vendor", "supplier", "client", "customer", "organization",
    "company", "corporation", "entity", "business", "department", "branch",
    "division", "region", "office", "headquarters", "hq", "subsidiary",
    "affiliate", "partner", "contract", "agreement", "terms", "conditions",
    "policy", "procedure", "manual", "document", "record", "entry", "log",
    "available balance", "opening balance", "closing balance", "previous balance",
    "running balance", "account number", "sort code", "bic", "bank code",
    "routing number", "transaction date", "posting date", "value date",
    "account holder", "beneficiary name", "account title", "depositor",
    "account summary", "statement balance", "daily balance", "interest earned",
    "interest charged", "service charge", "bank fee", "transaction fee",
    "monthly fee", "processing fee", "service fee", "atm withdrawal",
    "cash withdrawal", "atm fee", "atm deposit", "check deposit", "cash deposit",
    "online payment", "direct debit", "standing order", "recurring payment",
    "bill payment", "utility payment", "mobile banking", "internet banking",
    "e-banking", "net banking", "digital banking", "payment reference",
    "transaction reference", "reference number", "authorization code",
    "approval code", "payment method", "card payment", "credit card",
    "debit card", "visa", "mastercard", "amex", "maestro", "card number",
    "card type", "transaction code", "bank identifier", "institution code",
    "branch code", "swift bic", "swift number", "clearing house", "settlement date",
    "cleared funds", "uncleared balance", "posted transaction", "pending transaction",
    "authorized amount", "settled amount", "transaction amount", "original amount",
    "foreign currency", "exchange rate", "converted amount", "conversion rate",
    "exchange fee", "commission fee", "wire transfer", "domestic transfer",
    "international transfer", "sepa transfer", "swift transfer", "bank transfer",
    "fund transfer", "interbank transfer", "same day transfer", "immediate payment",
    "faster payment", "real-time payment", "clearing reference", "reversal",
    "refund", "reimbursed", "chargeback", "return", "adjustment", "correction",
    "interest adjustment", "fee adjustment", "balance adjustment", "posting reference",
    "batch reference", "batch number", "reference id", "statement reference",
    "transaction narrative", "transaction description", "narrative", "remarks",
    "memo", "comment", "statement line", "ledger entry", "posting entry",
    "account entry", "journal entry", "double entry", "trial balance",
    "general ledger", "subsidiary ledger", "debit entry", "credit entry",
    "posting date", "booking date", "settlement reference", "bank identifier code",
    "financial institution", "deposit account", "savings account", "fixed deposit",
    "term deposit", "call deposit", "corporate account", "business account",
    "joint account", "personal account", "trust account", "escrow account",
    "treasury account", "investment account", "loan account", "mortgage account",
    "credit line", "overdraft facility", "loan repayment", "loan disbursement",
    "loan interest", "loan principal", "repayment amount", "repayment schedule",
    "installment", "installment amount", "payment plan", "credit facility",
    "credit limit", "available credit", "used credit", "minimum payment",
    "statement cycle", "billing cycle", "billing period", "invoice number",
    "invoice date", "invoice amount", "invoice total", "invoice reference",
    "payment terms", "due balance", "past due", "arrears", "late fee",
    "penalty", "penalty charge", "collection fee", "service charge",
    "account maintenance", "maintenance fee", "annual fee", "renewal fee",
    "closing fee", "cancellation fee", "withdrawal fee", "deposit fee",
    "transaction charge", "transfer fee", "bank commission", "exchange commission",
    "processing time", "processing date", "transaction status", "completed payment",
    "pending payment", "failed payment", "declined payment", "returned payment",
    "disputed payment", "statement copy", "duplicate statement", "bank document",
    "account document", "statement page", "summary page", "details page",
    "account overview", "transaction summary", "account statement", "monthly summary",
    "financial summary", "financial statement", "cash statement", "fund statement",
    "consolidated statement", "investment statement", "loan statement",
    "credit statement", "debit statement", "mortgage statement", "deposit slip",
    "withdrawal slip", "check number", "check deposit", "check clearing",
    "check withdrawal", "issued check", "returned check", "dishonored check",
    "bounced check", "draft", "bank draft", "demand draft", "cashier's check",
    "manager's check", "certified check", "traveler's check", "postal order",
    "money order", "remittance", "international remittance", "wire instruction",
    "transfer instruction", "payment instruction", "transfer confirmation",
    "fund confirmation", "account verification", "balance verification",
    "statement verification", "bank confirmation", "confirmation letter",
    "remittance advice", "payment advice", "credit advice", "debit advice",
    "credit note", "debit note", "memo entry", "contra entry", "offset entry",
    "reversing entry", "bank reconciliation", "reconciliation statement",
    "outstanding check", "uncleared deposit", "pending deposit", "unpresented check",
    "cleared balance", "book balance", "bank balance", "adjusted balance",
    "cash book", "petty cash", "cash receipt", "cash payment", "bank receipt",
    "bank payment", "cash journal", "bank journal", "statement reconciliation",
    "interbank settlement", "clearing system", "settlement system", "payment network",
    "card network", "issuer bank", "acquirer bank", "interchange", "merchant id",
    "merchant name", "terminal id", "point of sale", "pos terminal", "pos transaction",
    "atm location", "atm id", "transaction channel", "online banking reference",
    "internet transaction", "mobile transaction", "branch transaction", "counter deposit",
    "teller id", "cashier id", "bank officer", "account officer", "relationship manager",
    "branch manager", "bank manager", "financial advisor", "investment officer",
    "loan officer", "credit officer", "treasury manager", "auditor", "accountant",
    "financial controller", "chief accountant", "statement footer", "statement header",
    "accounting period", "fiscal year", "calendar year", "quarter", "month end",
    "year end", "reporting date", "valuation date", "posting period", "reversal date",
    "due amount", "net amount", "gross amount", "tax amount", "interest amount",
    "principal amount", "fee amount", "charge amount", "total amount", "sub total",
    "grand total", "running total", "opening amount", "closing amount", "exchange amount",
    "converted value", "debit amount", "credit amount", "amount in words",
    "amount in figures", "statement total", "page total", "cumulative total",
    "account total", "balance carried forward", "balance brought forward",
    "forward balance", "b/f", "c/f", "credit balance", "debit balance",
    "net balance", "cleared balance", "available funds", "blocked funds",
    "held funds", "reserved funds", "statement note", "remarks section",
    "transaction remarks", "narrative text", "additional details", "payment reference number"
];

/** regexes (conservative) */
const regexes = {
    email: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/giu,
    phone: /(?:(?:\+|00)?\d{2,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}/g,
    cnp: /\b[1-9]\d{12}\b/g,
    card: /\b(?:\d[ -]?){13,19}\b/g,
    iban: /\b[A-Z]{2}\d{2}[A-Z0-9]{8,30}\b/g,
// Only redact 9+ digit numbers that don't look like YYYYMMDD or MMDDYYYY
    longnum: /\b(?!\d{4}(?:0[1-9]|1[0-2])(?:[0-3]\d)|(?:0[1-9]|1[0-2])[0-3]\d\d{4})\d{9,}\b/g,
    // date: /\b(?:0?[1-9]|[12][0-9]|3[01])[-\/.](?:0?[1-9]|1[0-2])[-\/.](?:19|20)\d{2}\b/g,
    name: /\b(?:Dna|Dl|Doamna|Domnul|Mr|Mrs|Ms|Miss|Sir|Madam)?\s*(?:[A-ZĂÂÎȘȚ][a-zăâîșț]+|[A-ZĂÂÎȘȚ]{2,})(?:[\s-](?:[A-ZĂÂÎȘȚ][a-zăâîșț]+|[A-ZĂÂÎȘȚ]{2,})){1,4}\b/gu
};

function collectPIIMatches(lineText) {
    const matches = [];
    for (const [type, re] of Object.entries(regexes)) {
        re.lastIndex = 0;
        let m;
        while ((m = re.exec(lineText)) !== null) {
            const txt = (m[0] || "").trim();
            if (!txt) continue;
            const lower = txt.toLowerCase();
            if (NON_PII_TERMS.some(t => lower.includes(t))) continue;
            matches.push({ type, text: txt, start: m.index, end: m.index + txt.length });
            if (m.index === re.lastIndex) re.lastIndex++;
        }
    }
    matches.sort((a, b) => a.start - b.start || b.end - a.end);
    return matches;
}

async function safeGetDocument(opts) {
    const origWarn = console.warn;
    try {
        console.warn = () => {
        };
        return await getDocument(opts).promise;
    } finally {
        console.warn = origWarn;
    }
}

function groupItemsIntoLines(itemsOrig) {
    const items = itemsOrig.map(it => {
        const t = it.transform || [1, 0, 0, 1, 0, 0];
        return { ...it, x: t[4], y: t[5], transform: t };
    }).sort((A, B) => {
        const dy = B.y - A.y;
        if (Math.abs(dy) > 3) return dy;
        return (A.x || 0) - (B.x || 0);
    });

    const lines = [];
    let current = [];
    let lastY = null;
    for (const it of items) {
        if (lastY === null || Math.abs(it.y - lastY) <= 3) {
            current.push(it);
        } else {
            current.sort((a, b) => (a.x || 0) - (b.x || 0));
            lines.push(current);
            current = [it];
        }
        lastY = it.y;
    }
    if (current.length) {
        current.sort((a, b) => (a.x || 0) - (b.x || 0));
        lines.push(current);
    }

    const result = lines.map(lineItems => {
        const mapping = [];
        let cursor = 0;
        for (let i = 0; i < lineItems.length; i++) {
            const it = lineItems[i];
            const str = it.str || "";
            const t = it.transform || [1, 0, 0, 1, 0, 0];
            const width = (typeof it.width === "number" && it.width > 0) ? it.width : Math.max(1, str.length * 6);
            const charWidth = width / Math.max(1, str.length);
            const d = Math.abs(t[3]) || Math.abs(t[0]) || 10;
            const fontHeight = (it.height && it.height > 0) ? it.height : d;
            const ascent = fontHeight * 0.72;
            const descent = fontHeight * 0.28;
            const start = cursor;
            cursor += str.length;
            const hasTrailingSpace = /\s$/.test(str);
            let insertedSpace = false;
            if (!hasTrailingSpace && i < lineItems.length - 1) {
                cursor += 1;
                insertedSpace = true;
            }
            const end = cursor;
            mapping.push({
                str,
                x: t[4],
                yBaseline: t[5],
                width,
                charWidth,
                fontHeight,
                ascent,
                descent,
                textStart: start,
                textEnd: end,
                insertedSpace
            });
        }
        const pieces = [];
        for (let i = 0; i < mapping.length; i++) {
            pieces.push(mapping[i].str);
            if (mapping[i].insertedSpace && i < mapping.length - 1) pieces.push(" ");
        }
        return { text: pieces.join(""), mapping };
    });

    return result;
}

/**
 * FIXED: Proper coordinate conversion with enhanced coverage
 * PDF.js and pdf-lib both use bottom-left origin for the coordinate system.
 * The yBaseline from PDF.js transform[5] is already in the correct coordinate system.
 */
function bboxFromMapping(matchStart, matchEnd, mapping) {
    let firstIdx = -1, lastIdx = -1;
    for (let i = 0; i < mapping.length; i++) {
        const m = mapping[i];
        if (m.textEnd <= matchStart) continue;
        if (m.textStart >= matchEnd) break;
        if (firstIdx === -1) firstIdx = i;
        lastIdx = i;
    }
    if (firstIdx === -1 || lastIdx === -1) return null;

    const firstM = mapping[firstIdx];
    const lastM = mapping[lastIdx];

    // X coordinates
    const startOffsetChars = Math.max(0, matchStart - firstM.textStart);
    const startX = firstM.x + startOffsetChars * firstM.charWidth;

    const endOffsetChars = Math.max(0, matchEnd - lastM.textStart);
    const endX = lastM.x + Math.min(endOffsetChars * lastM.charWidth, lastM.width);

    // Y coordinates - find the span of all involved text items
    let maxBaseline = -Infinity, minBaseline = Infinity;
    let maxAscent = 0, maxDescent = 0, maxFontHeight = 0;

    for (let i = firstIdx; i <= lastIdx; i++) {
        const m = mapping[i];
        if (m.yBaseline > maxBaseline) maxBaseline = m.yBaseline;
        if (m.yBaseline < minBaseline) minBaseline = m.yBaseline;
        if (m.ascent > maxAscent) maxAscent = m.ascent;
        if (m.descent > maxDescent) maxDescent = m.descent;
        if (m.fontHeight > maxFontHeight) maxFontHeight = m.fontHeight;
    }

    // Enhanced ascent and descent with safety margins
    // Increase ascent by 15% and descent by 20% for better coverage
    const enhancedAscent = maxAscent * 1.15;
    const enhancedDescent = maxDescent * 1.20;

    // The bounding box in pdf-lib coordinates (bottom-left origin)
    const bottom = minBaseline - enhancedDescent;
    const top = maxBaseline + enhancedAscent;

    const width = Math.max(1, endX - startX);
    const height = Math.max(1, top - bottom);

    return {
        x: startX,
        y: bottom,
        width,
        height
    };
}

async function pdfToImagePdf(pdfBytes) {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pdfimg-"));
    const inFile = path.join(tmpDir, "input.pdf");
    fs.writeFileSync(inFile, pdfBytes);

    // Convert to PNGs at 200 DPI
    await execPromise("pdftoppm", ["-png", "-r", "200", inFile, path.join(tmpDir, "page")]);

// Collect page images
    const images = fs.readdirSync(tmpDir)
        .filter(f => f.startsWith("page-") && f.endsWith(".png"))
        .map(f => path.join(tmpDir, f))
        .sort();

    const pdfDoc = await PDFDocument.create();
    for (const imgPath of images) {
        const imgBytes = fs.readFileSync(imgPath);
        const img = await pdfDoc.embedPng(imgBytes);
        const page = pdfDoc.addPage([img.width, img.height]);
        page.drawImage(img, { x: 0, y: 0, width: img.width, height: img.height });
    }

    const out = await pdfDoc.save();
    fs.rmSync(tmpDir, { recursive: true, force: true });
    return out;
}

function logWithMeta(label, meta) {
    const detail = meta ? ` ${JSON.stringify(meta)}` : "";
    console.log(`[StatementLog] ${label}${detail}`);
}

function logTiming(label, start, meta) {
    const elapsed = Math.max(0, nowMs() - start);
    const detail = meta ? ` ${JSON.stringify(meta)}` : "";
    console.log(`[StatementTiming] ${label} ${elapsed}ms${detail}`);
}

function nowMs() {
    return Date.now();
}

function deriveRedactedFilename(original) {
    const trimmed = original.trim();
    const dotIndex = trimmed.lastIndexOf(".");
    const base = dotIndex >= 0 ? trimmed.slice(0, dotIndex) : trimmed;
    const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, "_") || "statement";
    return `${safeBase}_redacted.pdf`;
}

async function uploadPdfToSupabaseStorage(buffer, folder, originalName, supabaseAdmin) {
    const bucket = "bankstatement2csv";
    const filename = deriveRedactedFilename(originalName);
    const objectPath = `${folder}/${filename}`;

    const upload = await supabaseAdmin.storage.from(bucket).upload(objectPath, buffer, {
        cacheControl: "3600",
        contentType: "application/pdf",
        upsert: false,
    });

    if (upload.error) {
        throw upload.error;
    }

    return {
        path: objectPath,
        filename,
        size: buffer.byteLength,
    };
}

function buildExtractionPrompt(batchLimit, cursor, thorough, history) {
    const recentHistory = history.slice(-5);

    let prompt = `Parse ALL transactions from the bank statement document pdf provided.
Return ONLY a JSON object: { "transactions": [ { "date": "YYYY-MM-DD", "debit": number, "credit": number, "category": string, "title": string, "note": string, "amount": number } ] }

Rules:
- Return at most ${batchLimit} new transactions in chronological order.
- Normalize date to YYYY-MM-DD; detect locale from context.
- Interpret Amount/Value/Charge columns: if a single column represents net amount, return positive values as debits and negative values (or credit indicators) as credits.
- If separate debit/credit columns exist, map them accordingly.
- Recognize alternate headings (Amount, Value, Withdrawal, Deposit, DR/CR, +/-).
- Convert comma/dot decimals correctly; do not lose decimals.
- Treat blank fields as zero; never drop a transaction row even if data seems incomplete.
- Title should summarize the payee/description.
- Category should be one of: Food, Transport, Shopping, Bills, Income, Other (choose best guess).
- Note can include original memo or additional context.
- Do NOT omit rows; ignore headers/balances/totals.
- If a row is ambiguous, still return best effort (never skip).
- Only return an empty array if the statement truly contains zero transactions.
- The array of transactions should be returned in the same order they appear in the pdf!`

    if (cursor) {
        prompt += `

Continue immediately after this transaction (do NOT repeat it or earlier rows):
${JSON.stringify(cursor)}

If you cannot locate this entry, advance until you find it and then continue forward.`;
    } else {
        prompt += `

Start from the earliest transaction in the statement.`;
    }

    if (recentHistory.length) {
        const historyLines = recentHistory.map((entry) => `- ${JSON.stringify(entry)}`).join("\n");
        prompt += `

These transactions are already captured:
${historyLines}
Only output rows that appear after the last entry in this list.`;
    }

    if (thorough) {
        prompt += `

The previous output repeated earlier rows. Carefully scan forward to find new pages or later rows before replying.`;
    }

    return prompt;
}

/**
 * Split a PDF into multiple chunks of specified page size
 */
async function splitPdfIntoChunks(buffer, pagesPerChunk = 5) {
    let payloadBuffer;
    if (Buffer.isBuffer(buffer)) {
        payloadBuffer = buffer;
    } else if (typeof buffer === "string") {
        payloadBuffer = Buffer.from(buffer, "base64");
    } else if (buffer instanceof ArrayBuffer) {
        payloadBuffer = Buffer.from(buffer);
    } else if (ArrayBuffer.isView(buffer)) {
        payloadBuffer = Buffer.from(buffer.buffer);
    } else {
        throw new Error("Unsupported buffer type");
    }

    // Load the PDF
    const pdfDoc = await PDFDocument.load(payloadBuffer);
    const totalPages = pdfDoc.getPageCount();
    console.log(`[PDF Split] Total pages: ${totalPages}, splitting into chunks of ${pagesPerChunk} pages`);

    const chunks = [];
    for (let startPage = 0; startPage < totalPages; startPage += pagesPerChunk) {
        const endPage = Math.min(startPage + pagesPerChunk, totalPages);
        
        // Create a new PDF document for this chunk
        const chunkDoc = await PDFDocument.create();
        
        // Copy pages from original PDF to chunk
        const copiedPages = await chunkDoc.copyPages(pdfDoc, Array.from({ length: endPage - startPage }, (_, i) => startPage + i));
        copiedPages.forEach(page => chunkDoc.addPage(page));
        
        // Save the chunk as a buffer
        const chunkBuffer = Buffer.from(await chunkDoc.save());
        
        chunks.push({
            buffer: chunkBuffer,
            startPage: startPage + 1, // 1-indexed for display
            endPage: endPage,
            pageCount: endPage - startPage
        });
        
        console.log(`[PDF Split] Created chunk ${chunks.length}: pages ${startPage + 1}-${endPage} (${endPage - startPage} pages, ${(chunkBuffer.length / 1024).toFixed(2)} KB)`);
    }

    return chunks;
}

/**
 * Upload a PDF chunk to Gemini Files API and wait for it to be ready
 */
async function uploadPdfChunkToGemini(chunkBuffer, displayName, mime) {
    const payloadBlob = new Blob([chunkBuffer], { type: mime || "application/pdf" });
    Object.defineProperty(payloadBlob, "size", { value: chunkBuffer.length });
    
    console.log(`[Gemini] Uploading chunk: ${displayName} (${(chunkBuffer.length / 1024 / 1024).toFixed(2)} MB)`);

    const upload = await googleAI.files.upload({
        file: payloadBlob,
        config: {
            mimeType: mime || "application/pdf",
            displayName: displayName,
        }
    });
    const fileName = upload?.name;
    const fileUri = upload?.uri;

    if (!fileName || !fileUri) {
        throw new Error("Failed to upload chunk to Gemini Files API");
    }

    console.log(`[Gemini] Chunk uploaded: ${fileName} (initial state: ${upload?.state || "UNKNOWN"})`);

    // Wait for file to be processed before using it
    let fileState = upload?.state || "PROCESSING";
    let attempts = 0;
    const maxAttempts = 60; // 2 minutes max wait

    while (fileState === "PROCESSING" && attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds
        const fileInfo = await googleAI.files.get({ name: fileName });
        fileState = fileInfo?.state || "ACTIVE";
        attempts++;

        if (fileState === "ACTIVE") {
            console.log(`[Gemini] Chunk ready after ${attempts * 2} seconds`);
            break;
        } else if (fileState === "FAILED") {
            throw new Error("Chunk processing failed in Gemini API");
        }
    }

    if (fileState !== "ACTIVE") {
        throw new Error(`Chunk not ready after ${maxAttempts * 2} seconds (state: ${fileState})`);
    }

    return { fileName, fileUri };
}

async function extractTransactionsFromStatement(buffer, filename, mime) {
    const BATCH_LIMIT = 250;
    const cursor = null;
    const history = [];
    const duplicateRuns = 0;

    const basePrompt = buildExtractionPrompt(BATCH_LIMIT, cursor, duplicateRuns > 0, history);

    const chunkingRules = `
Output rules:
1. Extract ALL transactions from this document chunk in chronological order.
2. Output a JSON object: { "transactions": [ ... ] }
3. Do NOT wrap JSON in code fences.
4. Return "END ---" followed by the JSON when you have extracted all transactions from this chunk.
`;

    console.log("[Gemini] Processing file:", filename);
    const fileDisplayName = filename || `statement-${Date.now()}.pdf`;

    const END_MARKER = "END ---";

    const parseChunk = (rawText) => {
        if (!rawText) return { transactions: [] };

        let cleaned = rawText.trim();
        
        // Remove END marker if present
        if (cleaned.startsWith(END_MARKER)) {
            cleaned = cleaned.slice(END_MARKER.length).trim();
        }
        
        // Remove all variations of markdown code fences and markers
        cleaned = cleaned
            .replace(/```json/gi, "")
            .replace(/```/g, "")
            .replace(/---\s*END\s*---/gi, "")
            .replace(/JsonArray|JsonObject|JsonValue/gi, "")
            .trim();

        // Find the first { and extract JSON from there
        const firstBrace = cleaned.indexOf('{');
        if (firstBrace < 0) {
            console.warn("[Gemini] No JSON object found in response");
            return { transactions: [] };
        }
        
        let candidateJson = cleaned.slice(firstBrace);
        
        // Find the last } to close the JSON object
        const lastBrace = candidateJson.lastIndexOf('}');
        if (lastBrace >= 0) {
            candidateJson = candidateJson.slice(0, lastBrace + 1);
        }

        if (!candidateJson) {
            return { transactions: [] };
        }

        // Try parsing as complete JSON
        try {
            const parsed = JSON.parse(candidateJson);
            if (parsed?.error) {
                console.warn("[Gemini] Chunk contains error payload", parsed.error);
                return { transactions: [] };
            }
            if (Array.isArray(parsed?.transactions)) {
                return { transactions: parsed.transactions };
            }
            console.warn("[Gemini] Parsed JSON lacks transactions array", parsed);
            return { transactions: [] };
        } catch (firstErr) {
            console.warn("[Gemini] JSON parse failed for chunk:", firstErr.message);
            console.warn("[Gemini] Attempted to parse:", candidateJson.slice(0, 500));
            return { transactions: [] };
        }
    };

    /**
     * Process a single PDF chunk with Gemini
     */
    const processChunkWithGemini = async (fileUri, chunkIndex, totalChunks, modelName) => {
        console.log(`[Gemini] [${modelName}] Processing chunk ${chunkIndex}/${totalChunks}`);
        
        const prompt = `${basePrompt}\n\n${chunkingRules}

This is chunk ${chunkIndex} of ${totalChunks} from the complete statement.
Extract ALL transactions from this chunk in chronological order.`;

        const conversationHistory = [
            {
                role: "user",
                parts: [
                    { text: prompt },
                    { fileData: { mimeType: mime || "application/pdf", fileUri } }
                ]
            }
        ];

        // Log the prompt being sent to Gemini
        console.log(`[Gemini] [${modelName}] chunk ${chunkIndex} request - conversation turns: ${conversationHistory.length}`);
        for (let i = 0; i < conversationHistory.length; i++) {
            const turn = conversationHistory[i];
            const isFileInPart = turn.parts?.some(p => p.fileData);
            const textContent = turn.parts?.find(p => p.text)?.text || "";
            const preview = textContent.length > 300 
                ? textContent.slice(0, 300) + "..." 
                : textContent;
            console.log(`  [Turn ${i + 1}] role: ${turn.role}${isFileInPart ? ' [+FILE]' : ''} | text: ${preview}`);
        }

        const timeoutMs = 300000; // 5 minutes per chunk

        try {
            const responsePromise = googleAI.models.generateContent({
                model: modelName,
                contents: conversationHistory,
            });

            const timeoutPromise = new Promise((_, reject) =>
                setTimeout(() => reject(new Error(`Timeout after ${timeoutMs / 1000}s`)), timeoutMs)
            );

            const response = await Promise.race([responsePromise, timeoutPromise]);

            const candidate = response.candidates?.[0];
            if (!candidate?.content) {
                console.warn(`[Gemini] [${modelName}] No candidate content for chunk ${chunkIndex}`);
                return [];
            }

            const responseText = candidate.content.parts
                ?.map((p) => p.text ?? "")
                .join("")
                .trim() || "";

            // Log response body with truncation (first 200 and last 200 chars)
            if (responseText) {
                const first200 = responseText.slice(0, 200);
                const last200 = responseText.slice(-200);
                const bodyPreview = responseText.length > 400
                    ? `${first200} ... ${last200}`
                    : responseText;
                console.log(`[Gemini] [${modelName}] chunk ${chunkIndex} response body:`, bodyPreview);
            } else {
                console.log(`[Gemini] [${modelName}] chunk ${chunkIndex} response body: (empty)`);
            }

            const { transactions } = parseChunk(responseText);
            console.log(`[Gemini] [${modelName}] chunk ${chunkIndex} extracted ${transactions.length} transactions`);
            
            return transactions;
        } catch (err) {
            console.error(`[Gemini] [${modelName}] Error processing chunk ${chunkIndex}:`, {
                message: err.message,
                name: err.name,
                code: err.code
            });
            
            // Retry once for network errors
            const isNetworkError = err.message?.includes('fetch failed') ||
                err.message?.includes('ECONNRESET') ||
                err.message?.includes('ETIMEDOUT') ||
                err.code === 'ECONNRESET' ||
                err.code === 'ETIMEDOUT';

            if (isNetworkError) {
                console.log(`[Gemini] [${modelName}] Retrying chunk ${chunkIndex} after network error...`);
                await new Promise(resolve => setTimeout(resolve, 5000));
                
                try {
                    const response = await googleAI.models.generateContent({
                        model: modelName,
                        contents: conversationHistory,
                    });
                    
                    const candidate = response.candidates?.[0];
                    const responseText = candidate?.content?.parts?.map(p => p.text ?? "").join("").trim() || "";
                    
                    // Log retry response
                    if (responseText) {
                        const first200 = responseText.slice(0, 200);
                        const last200 = responseText.slice(-200);
                        const bodyPreview = responseText.length > 400
                            ? `${first200} ... ${last200}`
                            : responseText;
                        console.log(`[Gemini] [${modelName}] chunk ${chunkIndex} retry response body:`, bodyPreview);
                    }
                    
                    const { transactions } = parseChunk(responseText);
                    console.log(`[Gemini] [${modelName}] chunk ${chunkIndex} retry extracted ${transactions.length} transactions`);
                    return transactions;
                } catch (retryErr) {
                    console.error(`[Gemini] [${modelName}] Retry failed for chunk ${chunkIndex}:`, retryErr.message);
                    throw retryErr;
                }
            }
            
            throw err;
        }
    };

    // Split PDF into chunks
    const chunks = await splitPdfIntoChunks(buffer, 5);
    console.log(`[Gemini] Split PDF into ${chunks.length} chunks`);

    // Upload all chunks to Gemini Files API
    const uploadedChunks = [];
    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const chunkDisplayName = `${fileDisplayName}_chunk_${i + 1}_pages_${chunk.startPage}-${chunk.endPage}`;
        
        try {
            const { fileName, fileUri } = await uploadPdfChunkToGemini(chunk.buffer, chunkDisplayName, mime);
            uploadedChunks.push({
                fileName,
                fileUri,
                chunkIndex: i + 1,
                startPage: chunk.startPage,
                endPage: chunk.endPage
            });
        } catch (uploadErr) {
            console.error(`[Gemini] Failed to upload chunk ${i + 1}:`, uploadErr.message);
            // Clean up already uploaded chunks
            for (const uploaded of uploadedChunks) {
                try {
                    await googleAI.files.delete({ name: uploaded.fileName });
                } catch (deleteErr) {
                    console.error(`[Gemini] Failed to delete chunk ${uploaded.fileName}:`, deleteErr.message);
                }
            }
            throw uploadErr;
        }
    }

    console.log(`[Gemini] All ${uploadedChunks.length} chunks uploaded successfully`);

    // Process all chunks and aggregate transactions
    const allTransactions = [];
    const seen = new Set();
    let totalDuplicates = 0;

    try {
        for (const uploadedChunk of uploadedChunks) {
            console.log(`[Gemini] Processing chunk ${uploadedChunk.chunkIndex}/${uploadedChunks.length} (pages ${uploadedChunk.startPage}-${uploadedChunk.endPage})`);
            
            let chunkTransactions = [];
            try {
                chunkTransactions = await processChunkWithGemini(
                    uploadedChunk.fileUri,
                    uploadedChunk.chunkIndex,
                    uploadedChunks.length,
                    "gemini-2.5-flash"
                );
            } catch (err) {
                console.error(`[Gemini] [gemini-2.5-flash] failed for chunk ${uploadedChunk.chunkIndex}, trying gemini-2.0-flash`);
                chunkTransactions = await processChunkWithGemini(
                    uploadedChunk.fileUri,
                    uploadedChunk.chunkIndex,
                    uploadedChunks.length,
                    "gemini-2.0-flash"
                );
            }

            // Deduplicate transactions across chunks
            let addedCount = 0;
            let chunkDuplicates = 0;
            
            for (const txn of chunkTransactions) {
                const key = JSON.stringify([
                    txn.date ?? "",
                    txn.title ?? "",
                    txn.debit ?? 0,
                    txn.credit ?? 0,
                    txn.amount ?? 0,
                    (txn.note ?? "").slice(0, 100),
                ]);
                if (!seen.has(key)) {
                    seen.add(key);
                    allTransactions.push(txn);
                    addedCount++;
                } else {
                    chunkDuplicates++;
                }
            }
            
            if (chunkDuplicates > 0) {
                console.warn(`[Gemini] Chunk ${uploadedChunk.chunkIndex} had ${chunkDuplicates} duplicates (extracted: ${chunkTransactions.length}, new: ${addedCount})`);
                totalDuplicates += chunkDuplicates;
            } else {
                console.log(`[Gemini] Chunk ${uploadedChunk.chunkIndex} added ${addedCount} new transactions (total: ${allTransactions.length})`);
            }
        }

        console.log(`[Gemini] Extraction complete: ${allTransactions.length} total transactions extracted (${totalDuplicates} duplicates filtered)`);
    } finally {
        // Clean up all uploaded chunks
        console.log(`[Gemini] Cleaning up ${uploadedChunks.length} uploaded chunks...`);
        for (const uploadedChunk of uploadedChunks) {
            try {
                await googleAI.files.delete({ name: uploadedChunk.fileName });
                console.log(`[Gemini] Deleted chunk file: ${uploadedChunk.fileName}`);
            } catch (deleteErr) {
                console.error(`[Gemini] Failed to delete chunk ${uploadedChunk.fileName}:`, deleteErr.message);
            }
        }
    }

    return allTransactions || [];
}

function normalizeTransactions(input) {
    return (Array.isArray(input) ? input : []).map((t) => {
        const date = (t?.date ?? "").toString();
        const category = (t?.category ?? "Other").toString();
        const note = (t?.note ?? "").toString();
        let debit = Number(t?.debit ?? 0);
        let credit = Number(t?.credit ?? 0);
        let amount = Number(t?.amount ?? 0);


        if (!isFinite(debit)) debit = 0;
        if (!isFinite(credit)) credit = 0;
        if (!isFinite(amount)) amount = 0;


        if (!debit && !credit) {
            let amountNum = Number(
                typeof t?.amount === "string"
                    ? t.amount.replace(/\s/g, "").replace(",", ".")
                    : t?.amount
            );
            if (!isFinite(amountNum)) amountNum = 0;
            const isIncome = amountNum < 0;
            if (isIncome) credit = Math.abs(amountNum);
            else debit = Math.abs(amountNum);
            amount = amountNum;
        }


        const title = (t?.title ?? "").toString();
        return { date, title, debit, credit, amount, category, note };
    });
}

async function redactPdf(pdfjsDoc, pdfDoc, debug) {
    for (let p = 0; p < pdfjsDoc.numPages; p++) {
        const pageNum = p + 1;
        const pdfjsPage = await pdfjsDoc.getPage(pageNum);
        const textContent = await pdfjsPage.getTextContent({ disableCombineTextItems: false });
        const pdfLibPage = pdfDoc.getPage(p);

        const itemsOrig = (textContent && textContent.items) || [];
        if (!itemsOrig.length) continue;

        const lines = groupItemsIntoLines(itemsOrig);

        for (const ln of lines) {
            const lineText = (ln.text || "").trim();
            if (!lineText) continue;

            const matches = collectPIIMatches(lineText);
            if (!matches.length) continue;

            // Merge overlapping matches
            const merged = [];
            for (const m of matches) {
                if (!merged.length) merged.push({ ...m });
                else {
                    const last = merged[merged.length - 1];
                    if (m.start <= last.end) {
                        last.end = Math.max(last.end, m.end);
                        last.text = lineText.slice(last.start, last.end);
                    } else merged.push({ ...m });
                }
            }

            for (const span of merged) {
                if (NON_PII_TERMS.some(t => span.text.toLowerCase().includes(t))) continue;

                // Split span if it contains large whitespace gaps (multiple names on same line)
                const spanText = lineText.slice(span.start, span.end);
                const subSpans = [];

                // Split by 2+ consecutive spaces or detect gaps in mapping
                let currentStart = span.start;
                let inWord = false;

                for (let i = span.start; i <= span.end; i++) {
                    const char = i < lineText.length ? lineText[i] : '';
                    const isSpace = !char || /\s/.test(char);

                    if (!isSpace && !inWord) {
                        // Starting a new word
                        currentStart = i;
                        inWord = true;
                    } else if ((isSpace || i === span.end) && inWord) {
                        // Check if there's a significant gap (multiple spaces or end)
                        let gapSize = 0;
                        for (let j = i; j < lineText.length && j < span.end && /\s/.test(lineText[j]); j++) {
                            gapSize++;
                        }

                        // End current word
                        if (i > currentStart) {
                            subSpans.push({ start: currentStart, end: i });
                        }
                        inWord = false;

                        // Skip significant gaps (3+ spaces suggests separate items)
                        if (gapSize >= 3) {
                            i += gapSize - 1;
                        }
                    }
                }

                // If no splits found, use the whole span
                if (subSpans.length === 0) {
                    subSpans.push({ start: span.start, end: span.end });
                }

                // Draw rectangle for each sub-span
                for (const subSpan of subSpans) {
                    const subText = lineText.slice(subSpan.start, subSpan.end).trim();
                    if (!subText || subText.length < 2) continue;

                    const box = bboxFromMapping(subSpan.start, subSpan.end, ln.mapping);
                    if (!box) continue;

                    // Enhanced horizontal padding to ensure first/last characters are fully covered
                    // Increase left padding more to cover first character
                    let padXLeft;
                    let padXRight;
                    let padY;
                    if (box.width < 30) {
                        padXLeft = Math.max(2.5, box.width * 0.35);
                        padXRight = Math.max(2, box.width * 0.035);
                        padY = 0.5;
                    } else {
                        padXLeft = Math.max(2.5, box.width * 0.09);
                        padXRight = Math.max(2, box.width * 0.035);
                        padY = 0.5;
                    }


                    pdfLibPage.drawRectangle({
                        x: box.x - padXLeft,
                        y: box.y - padY,
                        width: box.width + padXLeft + padXRight,
                        height: box.height + (2 * padY),
                        color: debug ? rgb(1, 0, 0) : rgb(0, 0, 0),
                        opacity: debug ? 0.45 : 1.0,
                    });
                }
            }
        }
    }

}

function toCsv(transactions) {
    const header = ["date", "title", "debit", "credit", "category", "note"].join(",");
    const escape = (value) => {
        const str = value == null ? "" : String(value);
        const needsQuotes = str.includes(",") || str.includes("\n") || str.includes('"');
        const escaped = str.replace(/"/g, '""');
        return needsQuotes ? `"${escaped}"` : escaped;
    };
    const rows = transactions.map((t) =>
        [
            escape(t.date),
            escape(t.title),
            t.debit,
            t.credit,
            escape(t.category),
            escape(t.note),
        ].join(",")
    );
    return [header, ...rows].join("\n");
}

function deriveCsvName(original) {
    const base = original.replace(/\.[^/.]+$/, "");
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    return `${base || "statement"}-${stamp}.csv`;
}

async function markJobReady(supabase, jobId, generatedFileId) {
    const { error } = await supabase
        .from("statement_jobs")
        .update({
            status: "ready",
            generated_file_id: generatedFileId,
            completed_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            error: null,
        })
        .eq("id", jobId);

    if (error) {
        throw error;
    }
}

async function markJobErrored(supabase, jobId, message) {
    await supabase
        .from("statement_jobs")
        .update({
            status: "error",
            error: message,
            updated_at: new Date().toISOString(),
        })
        .eq("id", jobId);
}

async function insertStatementJob(supabase, userId, file) {
    const { data, error } = await supabase
        .from("statement_jobs")
        .insert({
            user_id: userId,
            original_filename: file.name,
            format: file.format,
            status: "processing",
            credits_estimated: file.credits,
        })
        .select("*")
        .single();

    if (error || !data) {
        throw error ?? new Error("Failed to create job record");
    }

    return data;
}

async function countPdfPages(buffer) {
    try {
        const pdfDoc = await PDFDocument.load(buffer, { ignoreEncryption: true });
        const pages = pdfDoc.getPageCount();
        return pages > 0 ? pages : 1;
    } catch {
        return 1;
    }
}

async function uploadToGCS(buffer, filename, mime) {
    const id = uuidv4();
    const safeName = filename.replace(/[^a-zA-Z0-9.\-_]/g, "_");
    const objectName = `temp/${id}-${safeName}`;
    const bucket = storage.bucket(TEMP_BUCKET);
    const file = bucket.file(objectName);

    let bucketExists = true;
    try {
        const [exists] = await bucket.exists();
        bucketExists = Boolean(exists);
    } catch (existsErr) {
        console.error("[GCS upload] Bucket existence check failed", {
            bucket: TEMP_BUCKET,
            code: existsErr?.code,
            message: existsErr?.message,
        });
        throw existsErr;
    }

    if (!bucketExists) {
        throw new Error(`GCS bucket ${TEMP_BUCKET} does not exist or is inaccessible`);
    }

    const toBuffer = (input) => {
        if (Buffer.isBuffer(input)) return input;
        if (input instanceof Uint8Array) return Buffer.from(input);
        if (ArrayBuffer.isView(input)) return Buffer.from(input.buffer);
        if (input instanceof ArrayBuffer) return Buffer.from(input);
        throw new TypeError("uploadToGCS received unsupported buffer type");
    };

    const payload = toBuffer(buffer);

    try {
        await file.save(payload, {
        contentType: mime,
        resumable: false,
    });
    } catch (err) {
        console.error("[GCS upload] Failed", {
            bucket: TEMP_BUCKET,
            objectName,
            mime,
            byteLength: payload.byteLength,
            code: err?.code,
            message: err?.message,
            errors: err?.errors,
        });
        throw err;
    }

    const [url] = await file.getSignedUrl({
        action: "read",
        expires: Date.now() + 1000 * 60 * 60, // 1 hour temporary URL
    });

    return { gcsPath: `gs://${TEMP_BUCKET}/${objectName}`, url };
}


async function process(encodings, jobRecords, user, adminSupabase) {
    const downloadPayload = [];

    for (let index = 0; index < encodings.length; index += 1) {
        const file = encodings[index];
        const job = jobRecords[index];
        try {
            logWithMeta("Processing file", { filename: file.name });
            console.log(`Content redaction started for file ` + (file.name || "unknown"));

            const debug = false;
            const pdfBytes = file.buffer;
            const data = new Uint8Array(pdfBytes);
            const pdfjsDoc = await safeGetDocument({ data });
            const pdfDoc = await PDFDocument.load(pdfBytes, { ignoreEncryption: true }); // optional flag

            await redactPdf(pdfjsDoc, pdfDoc, debug);

            console.log(`🛡️ Content redacted successfuly for file ` + (file.name || "unknown"));

            console.log(`PDF redaction started for file ` + (file.name || "unknown"));

            const outBytes = await pdfDoc.save({
                useObjectStreams: false,
            });

            let redactedPdfBytes;
            try {
                redactedPdfBytes = await pdfToImagePdf(outBytes);
            } catch (conversionErr) {
                console.error("[Redaction] Failed to rasterize PDF via pdftoppm, falling back to vector PDF", conversionErr);
                redactedPdfBytes = outBytes;
            }
            console.log(`📄 PDF redacted successfuly for file ` + (file.name || "unknown"));

            console.log(job)
            const stored = await uploadPdfToSupabaseStorage(redactedPdfBytes.buffer ?? redactedPdfBytes, `${user.id}/${job.id}`, file.name, adminSupabase);
            logWithMeta("Stored redacted PDF in Supabase Storage Bucket", {
                filename: stored.filename,
                path: stored.path,
                size: stored.size
            });
            const jobUpdate = await adminSupabase
                .from("statement_jobs")
                .update({
                    redacted_pdf_path: stored.path,
                    redacted_pdf_name: stored.filename,
                    redacted_pdf_size: stored.size,
                    updated_at: new Date().toISOString(),
                })
                .eq("id", job.id);

            if (jobUpdate.error) {
                console.error("Failed to update job with redacted metadata", jobUpdate.error);
            }

            const extractionStart = nowMs();
            const transactionsRaw = await extractTransactionsFromStatement(redactedPdfBytes.buffer, file.name, file.mime);
            logTiming("extract.gemini", extractionStart, {
                filename: file.name,
                rows: transactionsRaw.length,
            });
            const transactions = normalizeTransactions(transactionsRaw);

            const csv = toCsv(transactions);
            const filename = deriveCsvName(file.name);

            const { data: inserted, error } = await adminSupabase
                .from("generated_files")
                .insert({
                    user_id: user.id,
                    filename,
                    size: Buffer.byteLength(csv, "utf-8"),
                    content: csv,
                    credits_used: file.credits,
                    source_pdf_path: stored.path,
                    source_pdf_filename: stored.filename,
                    source_pdf_size: stored.size,
                })
                .select("id, filename")
                .single();

            if (error || !inserted) {
                throw error ?? new Error("Failed to store generated file");
            }

            await markJobReady(adminSupabase, job.id, inserted.id);

            downloadPayload.push({
                name: inserted.filename,
                url: `https://bankstatement2csv.com/api/generated-files/${inserted.id}`,
            });
        } catch (err) {
            const message = err instanceof Error ? err.message : "Unknown error";
            console.error("Async processing failed for", file.name, err);
            await markJobErrored(adminSupabase, job.id, message);
            logWithMeta("Processing failed", { filename: file.name, error: message });
        }
    }

    if (downloadPayload.length) {
        try {
            // const html = renderProcessedEmail({
            //     userName: user.email,
            //     totalCredits,
            //     downloadUrls: downloadPayload,
            // });
            // await sendEmail({
            //     to: user.email,
            //     subject: "Your BankStatement2CSV conversions are ready",
            //     html,
            // });
            console.log('BankStatement2CSV conversions are ready for user', user.email);
        } catch (err) {
            console.error("Failed to send processing email", err);
        }
    }
}

const isLocal = false;
app.post("/process", verifyAuth, upload.array("files"), async (req, res) => {
    try {
        if (!req.files || !req.files.length) {
            return res.status(400).send("No files found in the request.");
        }

        const adminSupabase = createAdminClient();
        const supabase = await createSupabaseClient(req, res);
        const {
            data: { user },
            error,
        } = await supabase.auth.getUser();

        if (error) throw error;

        const filesMeta = [];

        for (const file of req.files) {
            const buffer = file.buffer;
            const name = file.originalname || "upload.pdf";
            const mime = file.mimetype || "application/octet-stream";
            const isPdf = mime === "application/pdf" || /\.pdf$/i.test(name);
            const credits = isPdf ? await countPdfPages(buffer) : 1;
            const { gcsPath, url } = await uploadToGCS(buffer, name, mime);

            filesMeta.push({
                name,
                mime,
                credits,
                isPdf,
                gcsPath,
                url,
            });
        }


        const jobRecords = [];
        for (const f of filesMeta) {
            console.log(`Creating job record for file ${f.name} (credits: ${f.credits})`);
            const job = await insertStatementJob(adminSupabase, user.id, {
                name: f.name,
                credits: f.credits,
                format: "csv",
            });
            jobRecords.push(job);
        }

        // === Extract Bearer token from request ===
        const authHeader = req.headers.authorization;
        const bearerToken = authHeader?.startsWith("Bearer ") ? authHeader.split(" ")[1] : null;

        if (!bearerToken) {
            return res.status(401).json({ error: "Missing authorization token" });
        }


        console.log('-------BODY-------------\n', JSON.stringify({
            jobRecords,
            files: filesMeta.map(({ name, mime, credits, gcsPath }) => ({ name, mime, credits, gcsPath })),
        }))


        if (isLocal) {
            fetch("http://localhost:8080/tasks/run/async", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${bearerToken}`,
                },
                body: JSON.stringify({
                    jobRecords,
                    files: filesMeta.map(({ name, mime, credits, gcsPath }) => ({ name, mime, credits, gcsPath })),
                }),
            }).then((resp) => {
                if (!resp.ok) {
                    return resp.text().then((t) => console.error("Task dispatch failed:", resp.status, t));
                } else {
                    res.json(resp.text());
                }
                console.log("Background task dispatched");
            }).catch((err) => {
                console.error("Failed to dispatch background task:", err);
            });
        } else {
        const taskPayload = {
            jobRecords,
                files: filesMeta.map(({ name, mime, credits, gcsPath }) => ({ name, mime, credits, gcsPath }))
            };
    
            const queuePath = tasksClient.queuePath(
                "bankstatement2csv",
                "us-central1",
                "pdf-jobs-queue"
            );

        const task = {
            httpRequest: {
                httpMethod: "POST",
                url: `https://bankstatement2csv.uc.r.appspot.com/tasks/run/async`,
                headers: {
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${bearerToken}`,
                },
                body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
            },
                dispatchDeadline: { seconds: 1800 }, // 30 minutes (max),
        };

            await tasksClient.createTask({ parent: queuePath, task });

        // === Respond immediately ===
        res.json(jobRecords);
        }
    } catch (err) {
        console.error("❌ Error:", err);
        if (!res.headersSent) {
            res.status(500).send("Error processing PDF: " + (err && err.message ? err.message : String(err)));
        }
    }
});

app.post("/tasks/run/async", async (req, res) => {
    try {
        console.log("🚀 Received Cloud Task for PDF processing with following payload");

        // === Extract user from Authorization header ===
        const authHeader = req.headers.authorization;
        const token = authHeader?.startsWith("Bearer ") ? authHeader.split(" ")[1] : null;

        if (!token) {
            console.error("❌ Missing authorization token in Cloud Task");
            return res.status(401).json({ error: "Missing authorization token" });
        }

        let user;
        try {
            user = await getUserFromToken(token);
        } catch (err) {
            console.error("❌ Invalid token in Cloud Task:", err.message);
            return res.status(401).json({ error: err.message });
        }

        const adminSupabase = createAdminClient();

        const requestBody = Buffer.isBuffer(req.body) ? req.body : Buffer.from(JSON.stringify(req.body || {}));
        const payload = JSON.parse(requestBody.toString());

        const { jobRecords, files } = payload || {};

        if (!Array.isArray(jobRecords) || !Array.isArray(files)) {
            throw new Error("Task payload missing jobRecords/files");
        }

        console.log("🚀 Received Cloud Task for PDF processing with following payload:", {
            jobRecordsCount: jobRecords.length,
            filesCount: files.length,
        });

        // === Respond immediately with 200 OK ===
        res.status(200).json({ status: "Task accepted", taskId: jobRecords[0]?.id });

        // === Process files in the background (don't await) ===
        (async () => {
            try {
                console.log(`📥 Fetching ${files.length} files from GCS for user ${user.email}`);
                const encodings = await Promise.all(
                    files.map(async (file) => {
                        try {
                            const bucket = storage.bucket(TEMP_BUCKET);
                            const objectPath = file.gcsPath.replace(`gs://${TEMP_BUCKET}/`, '');
                            const gcsFile = bucket.file(objectPath);
                            const [exists] = await gcsFile.exists();
                            if (!exists) {
                                throw new Error(`GCS object not found at gs://${TEMP_BUCKET}/${objectPath}`);
                            }

                            let buffer;
                            try {
                                [buffer] = await gcsFile.download();
                            } catch (downloadErr) {
                                console.error("[GCS download] Failed", {
                                    bucket: TEMP_BUCKET,
                                    objectPath,
                                    code: downloadErr?.code,
                                    message: downloadErr?.message,
                                });
                                throw downloadErr;
                            }

                            console.log(`✓ Downloaded ${file.name} (${buffer.length} bytes)`);
                            
                            return {
                                name: file.name,
                                mime: file.mime,
                                credits: file.credits,
                                buffer: buffer,
                            };
                        } catch (err) {
                            console.error(`❌ Failed to download ${file.name} from GCS:`, err.message);
                            throw err;
                        }
                    })
                );

                // === Process files ===
                await process(encodings, jobRecords, user, adminSupabase);

                console.log("✅ Background processing completed for user", user.email);
            } catch (err) {
                console.error("❌ Background task error for user", user.email, ":", err);
                // Update jobs to error status
                for (const job of jobRecords) {
                    await markJobErrored(adminSupabase, job.id, err.message || "Background processing failed");
                }
            }
        })();

    } catch (err) {
        console.error("❌ Task validation error:", err);
        if (!res.headersSent) {
            res.status(500).json({ error: "Task failed: " + err.message });
        }
    }
});

app.get("/liveness_check", (req, res) => res.status(200).send("ok"));
app.get("/readiness_check", (req, res) => res.status(200).send("ready"));
const port = 8080;
app.listen(port, () => console.log(`🚀 PDF Redaction service running on port ${port}`));
