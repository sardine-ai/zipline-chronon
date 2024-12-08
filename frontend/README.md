# Chronon Frontend

The frontend for Chronon.

## Getting Started

### Prerequisites

- [Node.js](https://nodejs.org/en/) (LTS version recommended)
- npm (comes with Node.js)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/zipline-ai/chronon.git
   cd chronon
   ```

2. Navigate to the frontend directory:

   ```bash
   cd frontend
   ```

3. Install dependencies:
   ```bash
   npm install
   ```

### Development

To start the development server:

1. First, start the backend project. Refer to [this doc](../docker-init/README.md) for more detailed info on
   building/running the backend.

```bash
# Run this command in the root directory
docker-init/build.sh --all
```

2. Once that command has finished, upload demo data:

```bash
# Run this command in the root directory
docker-init/demo/load_summaries.sh
...
Done uploading summaries! 🥳
```

3. Then, start the development server:

```bash
npm run dev
```

This will start a local server. The app will automatically reload if you make changes to the code.

### Build

To create an optimized production build:

```bash
npm run build
```

This will create an optimized version of your project in the `build` directory.

### Preview

To preview the production build locally:

```bash
npm run preview
```

### Running Tests

> **Important:** You must start the backend before running tests. In the future, we can add scripts to automatically start the backend before any tests are run.

#### All Tests

To run both unit and integration tests together:

```bash
npm run test
```

#### Unit Tests

To run unit tests using Vitest:

```bash
npm run test:unit
```

To run unit tests once:

```bash
npm run test:unit:once
```

#### Integration Tests

To run integration tests using Playwright:

```bash
npm run test:integration
```

To run integration tests once:

```bash
npm run test:integration:once
```

For the Playwright UI to explore test results:

```bash
npm run test:integration:ui
```

### Linting and Formatting

To check code formatting and linting issues:

```bash
npm run lint
```

To format the codebase:

```bash
npm run format
```

### Type Checking

To check the TypeScript types:

```bash
npm run check
```

To continuously check types while developing:

```bash
npm run check:watch
```

## Best Practices

1. **Code Style**: This project uses Prettier and ESLint for code formatting and linting. Please run `npm run lint` and `npm run format` before committing changes.
2. **Testing**: Ensure all changes are covered with unit and integration tests. Use Vitest for unit tests and Playwright for integration tests.
