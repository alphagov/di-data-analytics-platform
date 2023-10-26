import { handler } from './handler';
import type { TokenResponse, UserInfoResponse } from './handler';
import { mockApiGatewayEvent } from '../../shared/utils/test-utils';
import { mockClient } from 'aws-sdk-client-mock';
import { GenerateEmbedUrlForRegisteredUserCommand, QuickSightClient } from '@aws-sdk/client-quicksight';
import type { APIGatewayProxyEventV2 } from 'aws-lambda';
import { type AWS_ENVIRONMENTS } from '../../shared/constants';

const ACCOUNT_ID = '012345678901';

const CODE = '623b206c-ec61-439b-acc2-0e4a58cde86a';

const TOKEN_RESPONSE: TokenResponse = {
  id_token: 'id_token',
  access_token: 'access_token',
  refresh_token: 'refresh_token',
  expires_in: 3600,
  token_type: 'Bearer',
};

const USER_INFO_RESPONSE: UserInfoResponse = {
  sub: '07ad51f5-d89b-4936-9b8a-c9b24f7430be',
  email: 'test-user@digital.cabinet-office.gov.uk',
  email_verified: 'true',
  username: 'test-user',
};

const EMBED_URL = 'https://eu-west-2.quicksight.aws.amazon.com/embedding/.../start?code=...';

const mockQuicksightClient = mockClient(QuickSightClient);

let EVENT: APIGatewayProxyEventV2;

const setEvent = (event: APIGatewayProxyEventV2): void => {
  EVENT = event;
};

let COGNITO_CLIENT_ID: string;

const setClientId = (clientId: string): void => {
  COGNITO_CLIENT_ID = process.env.COGNITO_CLIENT_ID = clientId;
};

let COGNITO_DOMAIN: string;

const setDomain = (domain: string): void => {
  COGNITO_DOMAIN = process.env.COGNITO_DOMAIN = domain;
};

beforeEach(async () => {
  mockQuicksightClient.reset();
  setClientId('aR4nd0MCl1EntiD');
  setDomain('https://my-cognito-domain.auth.eu-west-2.amazoncognito.com');
  setEvent(await mockApiGatewayEvent({ code: CODE }, ACCOUNT_ID));
});

test('success', async () => {
  setUpSuccessfulFetch();
  const expectedArn = `arn:aws:quicksight:${process.env.AWS_REGION}:${ACCOUNT_ID}:user/default/${USER_INFO_RESPONSE.username}`;

  mockQuicksightClient
    .rejects()
    .on(GenerateEmbedUrlForRegisteredUserCommand, { AwsAccountId: ACCOUNT_ID, UserArn: expectedArn })
    .resolves({ EmbedUrl: EMBED_URL });

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 302,
    headers: {
      Location: EMBED_URL,
    },
  });

  expect(mockQuicksightClient.calls()).toHaveLength(1);
});

test('bad query parameters', async () => {
  setEvent(await mockApiGatewayEvent({ hello: 'world' }, ACCOUNT_ID));

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 400,
    body: JSON.stringify({
      error: 'code query param is missing or invalid - parameters are {"hello":"world"}',
    }),
  });

  expect(mockQuicksightClient.calls()).toHaveLength(0);
});

test('missing cognito client id', async () => {
  setClientId('');

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 500,
    body: JSON.stringify({
      error: 'COGNITO_CLIENT_ID is not defined in this environment',
    }),
  });

  expect(mockQuicksightClient.calls()).toHaveLength(0);
});

test('missing cognito domain', async () => {
  setDomain('');

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 500,
    body: JSON.stringify({
      error: 'COGNITO_DOMAIN is not defined in this environment',
    }),
  });

  expect(mockQuicksightClient.calls()).toHaveLength(0);
});

test('bad fetch', async () => {
  const error = {
    status: 504,
    statusText: 'Gateway Timeout',
    message: 'Did not get a response in time from the upstream server',
  };

  global.fetch = jest.fn().mockResolvedValueOnce({
    ok: false,
    status: error.status,
    statusText: error.statusText,
    text: async () => error.message,
  });

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 500,
    body: JSON.stringify({
      error: `${error.status} ${error.statusText} error calling token endpoint - ${error.message}`,
    }),
  });

  expect(mockQuicksightClient.calls()).toHaveLength(0);
});

test('quicksight error', async () => {
  global.fetch = jest
    .fn()
    .mockResolvedValueOnce({ ok: true, json: async () => TOKEN_RESPONSE })
    .mockResolvedValueOnce({ ok: true, json: async () => USER_INFO_RESPONSE });

  const expectedArn = `arn:aws:quicksight:${process.env.AWS_REGION}:${ACCOUNT_ID}:user/default/${USER_INFO_RESPONSE.username}`;

  const errorMessage = 'Quicksight error';
  mockQuicksightClient.rejects(errorMessage);

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 500,
    body: JSON.stringify({
      error: `Error getting quicksight embed url for userArn ${expectedArn} - "${errorMessage}"`,
    }),
  });

  expect(mockQuicksightClient.calls()).toHaveLength(1);
});

test('undefined embed url', async () => {
  global.fetch = jest
    .fn()
    .mockResolvedValueOnce({ ok: true, json: async () => TOKEN_RESPONSE })
    .mockResolvedValueOnce({ ok: true, json: async () => USER_INFO_RESPONSE });

  const expectedArn = `arn:aws:quicksight:${process.env.AWS_REGION}:${ACCOUNT_ID}:user/default/${USER_INFO_RESPONSE.username}`;

  mockQuicksightClient.resolves({ EmbedUrl: undefined });

  const response = await handler(EVENT);
  expect(response).toBeDefined();
  expect(response).toEqual({
    statusCode: 500,
    body: JSON.stringify({
      error: `Error getting quicksight embed url for userArn ${expectedArn} - "EmbedUrl is undefined"`,
    }),
  });

  expect(mockQuicksightClient.calls()).toHaveLength(1);
});

test('session duration', async () => {
  const expectSessionDuration = async (
    environment: (typeof AWS_ENVIRONMENTS)[number],
    expectedDuration: number,
  ): Promise<void> => {
    setUpSuccessfulFetch();
    process.env.ENVIRONMENT = environment;
    const response = await handler(EVENT);
    expect(response).toBeDefined();
    const embedUrl = (response as unknown as { headers: { Location: string } }).headers.Location;
    expect(embedUrl).toEqual(expectedDuration.toString());
  };

  // slight abuse of the EmbedUrl to hold the minutes passed in, but it is the easiest way to test this
  mockQuicksightClient
    .on(GenerateEmbedUrlForRegisteredUserCommand)
    .callsFake(async input => ({ EmbedUrl: input.SessionLifetimeInMinutes.toString() }));

  await expectSessionDuration('dev', 15);
  await expectSessionDuration('test', 15);
  await expectSessionDuration('feature', 15);
  await expectSessionDuration('production', 600);

  expect(mockQuicksightClient.calls()).toHaveLength(4);
});

const setUpSuccessfulFetch = (): void => {
  global.fetch = jest
    .fn()
    .mockImplementationOnce(async (url: string, init: RequestInit) => {
      expect(url).toEqual(`${COGNITO_DOMAIN}/oauth2/token`);
      expect(init.headers).toEqual({
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      });
      expect(init.body).toEqual(
        new URLSearchParams({
          grant_type: 'authorization_code',
          client_id: COGNITO_CLIENT_ID,
          redirect_uri: `https://${EVENT.requestContext.domainName}`,
          code: CODE,
        }),
      );
      return { ok: true, json: async () => TOKEN_RESPONSE };
    })
    .mockImplementationOnce(async (url: string, init: RequestInit) => {
      expect(url).toEqual(`${COGNITO_DOMAIN}/oauth2/userInfo`);
      expect(init.headers).toEqual({
        Authorization: `Bearer ${TOKEN_RESPONSE.access_token}`,
      });
      expect(init.body).not.toBeDefined();
      return { ok: true, json: async () => USER_INFO_RESPONSE };
    });
};
