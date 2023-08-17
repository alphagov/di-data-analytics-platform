export const envName = (): string => process.env.ENVIRONMENT ?? 'dev';

export const sqsQueueName = (): string => `${envName()}-placeholder-txma-event-queue`;

export const deliveryStreamName = (): string => `${envName()}-dap-txma-delivery-stream`;

export const rawdataS3BucketName = (): string => `${envName()}-dap-raw-layer`;

export const txmaStageDatabaseName = (): string => `${envName()}-txma-stage`;

export const txmaProcessingWorkGroupName = (): string => `${envName()}-dap-txma-processing`;

export const stageProcessStepFucntionName = (): string => `${envName()}-dap-raw-to-stage-process`;