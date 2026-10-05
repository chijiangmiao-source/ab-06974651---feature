FROM node:20-alpine
WORKDIR /app
COPY app/ app/
COPY server/ server/
COPY scripts/ scripts/
RUN node scripts/build.js
ENV PORT=8080
EXPOSE 8080
CMD ["node", "server/server.js"]
