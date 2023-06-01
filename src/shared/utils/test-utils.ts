import type { SQSEvent } from 'aws-lambda';
import { readFile } from 'fs/promises';

export const mockSQSEvent = (...bodies: unknown[]): SQSEvent => {
  return {
    Records: bodies.map((body, index) => ({
      body,
      messageId: (index + 1).toString(),
    })),
  } as unknown as SQSEvent;
};

export const getTestResource = async (filename: string): Promise<string> => {
  return await readFile(`src/test-resources/${filename}`, 'utf-8');
};
