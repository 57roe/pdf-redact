# =========================
# 1️⃣  BUILD STAGE
# =========================
FROM node:20-slim AS build

# Install only what’s needed to build native deps
RUN apt-get update && apt-get install -y \
    build-essential \
    libcairo2-dev \
    libpango1.0-dev \
    libjpeg-dev \
    libgif-dev \
    librsvg2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package files and install production deps only
COPY package*.json ./
RUN npm ci --omit=dev

# Copy app source
COPY . .

# =========================
# 2️⃣  RUNTIME STAGE
# =========================
FROM node:20-slim

# Install only runtime libraries (no compilers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcairo2 \
    libpango-1.0-0 \
    libjpeg62-turbo \
    libgif7 \
    librsvg2-2 \
    tesseract-ocr \
    poppler-utils \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (good security practice)
RUN useradd -m appuser
USER appuser

WORKDIR /app

# Copy prebuilt node_modules from build stage
COPY --from=build /app/node_modules ./node_modules

# Copy the rest of the built app
COPY --from=build /app .

ENV NODE_ENV=production
ENV PORT=8080
EXPOSE 8080

# Start the app
CMD ["node", "server.js"]
