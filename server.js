import express from "express";
import multer from "multer";
import fs from "fs";
import { PDFDocument, rgb } from "pdf-lib";
import pdfjsLib from "pdfjs-dist/legacy/build/pdf.js"; // dacă ai versiunea 2.16.105
import * as fontkit from "fontkit";


const { getDocument } = pdfjsLib;
const app = express();
const upload = multer({ dest: "uploads/" });
const PORT = 4000;

function redactPII(text) {
    return text
        // Emailuri
        .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, "[REDACTED_EMAIL]")
        // Numere de telefon
        .replace(/\b(\+?\d{1,3}[-.\s]?)?(\(?\d{2,4}\)?[-.\s]?)\d{3,4}[-.\s]?\d{3,4}\b/g, "[REDACTED_PHONE]")
        // CNP (România)
        .replace(/\b\d{13}\b/g, "[REDACTED_CNP]")
        // SSN (SUA)
        .replace(/\b\d{3}-\d{2}-\d{4}\b/g, "[REDACTED_SSN]")
        // NIN (UK)
        .replace(/\b[A-CEGHJ-PR-TW-Z]{2}\d{6}[A-D]\b/gi, "[REDACTED_NIN]")
        // AADHAAR (India)
        .replace(/\b\d{4}\s\d{4}\s\d{4}\b/g, "[REDACTED_AADHAAR]")
        // Carduri bancare
        .replace(/\b(?:\d[ -]*?){13,16}\b/g, "[REDACTED_CARD]")
        // IBAN
        .replace(/\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b/g, "[REDACTED_IBAN]")
        // Sume de bani
        .replace(/(\$|€|£)\s?\d+(?:[.,]\d{1,2})?/g, "[REDACTED_AMOUNT]")
        // Adrese
        .replace(/\b\d{1,5}\s+([A-Z][a-z]+\s?)+(Street|St|Road|Rd|Avenue|Ave|Boulevard|Blvd|Str|Strada|Calle|Rue)\b/gi, "[REDACTED_ADDRESS]")
        // Nume proprii
        .replace(/(?<![A-Za-z-])([A-Z][a-z]+(?:-[A-Z][a-z]+)*(\s+([A-Z][a-z]+(?:-[A-Z][a-z]+)*))+)(?![a-z-])/g, "[REDACTED_NAME]")
        // Date (DD/MM/YYYY sau YYYY-MM-DD)
        .replace(/\b(?:(0?[1-9]|[12][0-9]|3[01])[\/.-](0?[1-9]|1[0-2])[\/.-](19|20)\d{2}|\b(19|20)\d{2}-((0[1-9]|1[0-2]))-((0[1-9]|[12][0-9]|3[01]))\b)/g, "[REDACTED_DATE]");
}

app.post("/redact", upload.single("file"), async (req, res) => {
    try {
        const filePath = req.file.path;
        console.log("➡️ Uploaded file:", filePath);

        // ✅ Citim bytes și verificăm PDF header-ul
        const fileBytes = fs.readFileSync(filePath);
        const firstBytes = fileBytes.slice(0, 10).toString();
        console.log("🧩 First bytes:", firstBytes);

        if (!firstBytes.includes("%PDF")) {
            throw new Error("Invalid PDF file — missing %PDF header!");
        }

        const fontBytes = fs.readFileSync("fonts/arial.ttf"); // font complet Unicode
        const data = new Uint8Array(fileBytes);

        // 1️⃣ Încarcă PDF cu pdfjs-dist pentru extragere text
        const loadingTask = getDocument({ data });
        const pdf = await loadingTask.promise;

        // 2️⃣ Încarcă PDF în pdf-lib pentru modificare
        const pdfDoc = await PDFDocument.load(fileBytes);
        pdfDoc.registerFontkit(fontkit);
        const customFont = await pdfDoc.embedFont(fontBytes);

        for (let pageIndex = 0; pageIndex < pdf.numPages; pageIndex++) {
            const page = await pdf.getPage(pageIndex + 1);
            const { width, height } = page.getViewport({ scale: 1 });
            const pdfLibPage = pdfDoc.getPage(pageIndex);

            const textContent = await page.getTextContent();
            for (const item of textContent.items) {
                const rawText = item.str;
                const redactedText = redactPII(rawText);

                if (rawText !== redactedText) {
                    const [a, b, c, d, e, f] = item.transform;
                    const x = e;
                    const y = f;
                    const width = a;
                    const height = d;

                    const pageHeight = page.view[3];
                    const correctedY = pageHeight - y;

                    // Albim zona veche
                    pdfLibPage.drawRectangle({
                        x,
                        y: correctedY - height,
                        width: item.width || width,
                        height: item.height || height,
                        color: rgb(1, 1, 1),
                    });

                    pdfLibPage.drawText(redactedText, {
                        x,
                        y: correctedY - height * 0.8,
                        size: height * 0.8,
                        font: customFont,
                        color: rgb(0, 0, 0),
                    });

                }
            }
        }

        const pdfBytes = await pdfDoc.save();
        res.contentType("application/pdf");
        res.send(Buffer.from(pdfBytes));

        // 🧹 Curățăm fișierul temporar
        fs.unlinkSync(filePath);
    } catch (err) {
        console.error("❌ Error processing PDF:", err);
        res.status(500).send("Error processing PDF: " + err.message);
    }
});

app.listen(PORT, () => {
    console.log(`PDF PII microservice running on port ${PORT} ✅`);
});
