import express from "express";
import multer from "multer";
import {PDFDocument, rgb} from "pdf-lib";
import pkg from "pdfjs-dist/legacy/build/pdf.js";

const {getDocument} = pkg;
import fs from "fs";
import path from "path";
import os from "os";
import {exec} from 'child_process';
import * as util from 'util';

const upload = multer();
const app = express();

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
            resolve({stdout, stderr});
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

        console.log('Conversion successful:', result.stdout);
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
    "tranzactie", "ing"
];

/** regexes (conservative) */
const regexes = {
    email: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/giu,
    phone: /(?:(?:\+|00)?\d{2,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}/g,
    cnp: /\b[1-9]\d{12}\b/g,
    card: /\b(?:\d[ -]?){13,19}\b/g,
    iban: /\b[A-Z]{2}\d{2}[A-Z0-9]{8,30}\b/g,
    longnum: /\b\d{8,}\b/g,
    date: /\b(?:0?[1-9]|[12][0-9]|3[01])[-\/.](?:0?[1-9]|1[0-2])[-\/.](?:19|20)\d{2}\b/g,
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
            matches.push({type, text: txt, start: m.index, end: m.index + txt.length});
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
        return {...it, x: t[4], y: t[5], transform: t};
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
        return {text: pieces.join(""), mapping};
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
        page.drawImage(img, {x: 0, y: 0, width: img.width, height: img.height});
    }

    const out = await pdfDoc.save();
    fs.rmSync(tmpDir, {recursive: true, force: true});
    return out;
}


app.post("/redact", upload.single("file"), async (req, res) => {
    try {
        if (!req.file) return res.status(400).send("PDF lipsă.");

        const debug = Boolean(req.query && req.query.debug && req.query.debug !== "0");
        const pdfBytes = req.file.buffer;
        const data = new Uint8Array(pdfBytes);

        const pdfjsDoc = await safeGetDocument({data});
        const pdfDoc = await PDFDocument.load(pdfBytes);

        for (let p = 0; p < pdfjsDoc.numPages; p++) {
            const pageNum = p + 1;
            const pdfjsPage = await pdfjsDoc.getPage(pageNum);
            const textContent = await pdfjsPage.getTextContent({disableCombineTextItems: false});
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
                    if (!merged.length) merged.push({...m});
                    else {
                        const last = merged[merged.length - 1];
                        if (m.start <= last.end) {
                            last.end = Math.max(last.end, m.end);
                            last.text = lineText.slice(last.start, last.end);
                        } else merged.push({...m});
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
                                subSpans.push({start: currentStart, end: i});
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
                        subSpans.push({start: span.start, end: span.end});
                    }

                    // Draw rectangle for each sub-span
                    for (const subSpan of subSpans) {
                        const subText = lineText.slice(subSpan.start, subSpan.end).trim();
                        if (!subText || subText.length < 2) continue;

                        const box = bboxFromMapping(subSpan.start, subSpan.end, ln.mapping);
                        if (!box) continue;

                        // Enhanced horizontal padding to ensure first/last characters are fully covered
                        // Increase left padding more to cover first character
                        console.log('--------------------Box.width----------------', box.width);
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

                        console.log(`🛡️ Page ${pageNum} redacted "${subText}" (${span.type})`);
                    }
                }
            }
        }

        const outBytes = await pdfDoc.save();
        const imagePdfBytes = await pdfToImagePdf(outBytes);

        res.contentType("application/pdf");
        res.send(Buffer.from(imagePdfBytes));
    } catch (err) {
        console.error("❌ Error:", err);
        res.status(500).send("Eroare la procesarea PDF-ului: " + (err && err.message ? err.message : String(err)));
    }
});

app.listen(4000, () => console.log("🚀 PDF Redaction service running on port 4000"));