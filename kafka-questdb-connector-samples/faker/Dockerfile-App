FROM node:14
ARG NODE_ENV
ENV NODE_ENV $NODE_ENV

WORKDIR /usr/src/app
COPY package*.json ./

RUN npm install --only=production

COPY . .
CMD ["node", "index.js"]