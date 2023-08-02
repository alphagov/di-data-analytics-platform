import { InvokeCommand } from '@aws-sdk/client-lambda';
import type { TestSupportEnvironment, TestSupportEvent } from '../../src/handlers/test-support/handler';
import { decodeObject, encodeObject } from '../../src/shared/utils/utils';
import { lambdaClient } from '../../src/shared/clients';
import {invokeTestSupportLambda} from "./lambda-helpers";

//update the db name as per configiration
const database = "testdatabase";

export const getDataFromAthena = async (query: string): Promise<unknown> => {
  const event: Omit<TestSupportEvent, 'environment'> = {
    command: 'ATHENA_RUN_QUERY',
    input: {
      QueryString:  query,
      QueryExecutionContext: {
        "Database": database
      },
      WorkGroup: "test-workgroup"
    },
  };

  return await invokeTestSupportLambda(event);
};
