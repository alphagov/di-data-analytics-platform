'use strict';

import {
  DescribeAssetBundleExportJobCommand,
  DescribeAssetBundleImportJobCommand,
  ListDashboardsCommand,
  QuickSightClient,
  StartAssetBundleExportJobCommand,
  StartAssetBundleImportJobCommand,
} from '@aws-sdk/client-quicksight';

const quicksightClient = new QuickSightClient({ region: 'eu-west-2' });

export const handler = async event => {
  console.log('event', event);
  try {
    return await importOrExport(event);
  } catch (error) {
    console.error(`Error performing Quicksight ${event.action} in account ${event.accountId}`, error);
    throw error;
  }
};

const importOrExport = async event => {
  const jobId = Math.random().toString(36).substring(2);
  let startRequest;
  if (event.action === 'EXPORT') {
    const dashboardArn = await getDashboardArn(event);
    startRequest = new StartAssetBundleExportJobCommand({
      AwsAccountId: event.accountId,
      AssetBundleExportJobId: jobId,
      ResourceArns: [dashboardArn],
      ExportFormat: 'QUICKSIGHT_JSON',
      IncludeAllDependencies: true,
    });
  } else {
    startRequest = new StartAssetBundleImportJobCommand({
      AwsAccountId: event.accountId,
      AssetBundleImportJobId: jobId,
      AssetBundleImportSource: { S3Uri: event.s3Uri },
      FailureAction: 'ROLLBACK',
    });
  }

  console.log('startRequest', startRequest);
  await quicksightClient.send(startRequest);

  let describeRequest;
  if (event.action === 'EXPORT') {
    describeRequest = new DescribeAssetBundleExportJobCommand({
      AwsAccountId: event.accountId,
      AssetBundleExportJobId: jobId,
    });
  } else {
    describeRequest = new DescribeAssetBundleImportJobCommand({
      AwsAccountId: event.accountId,
      AssetBundleImportJobId: jobId,
    });
  }
  console.log('describeRequest', describeRequest);
  return await waitForSuccess(() => quicksightClient.send(describeRequest), 20000);
};

const getDashboardArn = async event => {
  let request = new ListDashboardsCommand({
    AwsAccountId: event.accountId,
  });

  while (request.NextToken !== null) {
    const response = await quicksightClient.send(request);
    if (response.DashboardSummaryList === undefined || response.DashboardSummaryList.length === 0) {
      throw new Error('List of dashboards undefined or empty');
    }

    const dashboard = response.DashboardSummaryList.find(dash => event.dashboardName === dash.Name);
    if (dashboard !== undefined) {
      return dashboard.Arn;
    }
    request = { ...request, NextToken: response.NextToken };
  }
  throw new Error('Searched all dashboards without a match');
};

const waitForSuccess = async (describeSupplier, timeoutMs) => {
  let jobDescription;
  let timeRemaining = timeoutMs;
  while (timeRemaining > 0) {
    jobDescription = await describeSupplier();
    if (jobDescription.JobStatus === 'SUCCESSFUL') {
      return jobDescription;
    } else if (jobDescription.JobStatus.startsWith('FAILED')) {
      break;
    }
    timeRemaining -= 200;
    await sleep(200);
  }
  console.error('Status', jobDescription?.Status);
  console.error('Errors', jobDescription?.Errors);
  console.error('RollbackErrors', jobDescription?.RollbackErrors);
  const duration = timeoutMs - timeRemaining;
  throw new Error(`Job failed after ${duration}ms - final description was ${JSON.stringify(jobDescription)}`);
};

const sleep = async ms => await new Promise(resolve => setTimeout(resolve, ms));
