FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

RUN mkdir -p data

EXPOSE 4242

CMD ["npx", "tsx", "src/server.ts"]
