module.exports = async () => {
  return {
    roots: ['<rootDir>/tests/e2e-tests'],
    testMatch: ['**/tests/e2e-tests/*.spec.ts'],
    coveragePathIgnorePatterns: ['/node_modules/'],
    // globalSetup: "./src/handlers/int-test-support/helpers/testSetup.ts",
    testTimeout: 300000,
    verbose: true,
    transform: {
      '^.+\\.tsx?$': '@swc/jest',
    },
    collectCoverage: true,
    testResultsProcessor: 'jest-junit',
    reporters: [
      'default',
      [
        'jest-junit',
        {
          suiteName: 'Test report',
          outputDirectory: '<rootDir>/reports',
          outputName: 'testReport.xml',
          ancestorSeparator: ',',
          includeConsoleOutput: true,
        },
      ],
    ],
    setupFiles: ['<rootDir>/jest.setup.js'],
  };
};
