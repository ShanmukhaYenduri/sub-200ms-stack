# Pinned minor version. "node:20" moves under you and takes your
# benchmark numbers with it.
FROM node:20.15-alpine

WORKDIR /app

# Manifests first so the dependency layer is cached independently of source.
# Editing src/server.js should not trigger a reinstall.
COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force

COPY . .

ENV NODE_ENV=production

# The API writes nothing to the filesystem, so it has no business running as
# root.
USER node

EXPOSE 3000

# No shell form: exec form makes node PID 1 so it receives SIGTERM directly and
# the graceful shutdown handler in src/server.js actually runs.
CMD ["node", "src/server.js"]
