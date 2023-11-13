interface ImportExportEvent {
  action: 'EXPORT' | 'IMPORT';
  accountId: string;
}

interface ExportEvent extends ImportExportEvent {
  dashboardName: string;
}

interface ImportEvent extends ImportExportEvent {
  s3Uri: string;
}
