ARG ALPINE_VERSION=3.16

FROM node:16-alpine${ALPINE_VERSION} AS run-tests

#WORKDIR /test-app

#COPY package.json ./
#COPY package-lock.json ./
#COPY tsconfig.json ./
#COPY tests tests
#COPY scripts scripts
#COPY src/handlers/test-support/ src/handlers/test-support/
#COPY src/shared/ src/shared/

COPY . .

RUN npm ci

RUN cp tests/scripts/run-tests.sh /run-tests.sh
RUN chmod +x /run-tests.sh

## To run unit-tests for starters
#WORKDIR /test-app
#COPY jest.config.cjs ./
#COPY test-helpers ./test-helpers
#COPY src ./src

#ENV TEST_VIA_LAMBDA=true
CMD ["sh", "/run-tests.sh"]
