# ─── STAGE 1: COMPILER ───
FROM node:22-alpine AS compiler
WORKDIR /app

# Install dependencies first to maximize Docker layer caching
COPY package*.json tsconfig.json ./
RUN npm install

# Copy source files and compile TypeScript into raw JavaScript
COPY src ./src
RUN npx tsc

# ─── STAGE 2: PRODUCTION RUNTIME ───
FROM node:22-alpine AS runtime
WORKDIR /app

# Install production-only dependencies (excludes development compilers)
COPY package*.json ./
RUN npm install --production

# CRITICAL FIX: Copy the entire compiled JavaScript build tree
COPY --from=compiler /app/dist ./dist

# Expose ports for API routing and storage interfaces
EXPOSE 5000 5001

# Default command fallback (overridden dynamically by docker-compose)
CMD ["node", "dist/storageServer.js"]