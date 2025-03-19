# Overview

We are using Temporal mainly for the following functionality
- Durable execution which can handle all kinds of failures
- Easy to set up schedules or timers for our workflows

## NodeExecutionWorkflow

The `NodeExecutionWorkflow` is responsible for coordinating the execution of nodes in a dependency graph. It works in conjunction with `NodeExecutionActivity` to ensure that all dependencies are satisfied before executing a node's job.

### Workflow Execution Process

```
NodeExecutionWorkflow
├── Depends on: NodeExecutionActivity
├── find missing partitions and compute missing steps from step days
    └── for each missing step in missing steps
        └── triggerDependencies
            └── Creates child workflows for dependencies (recursive)
└── Wait for all dependencies to finish
└── submitJob
    └── Submits job to agent task queue
```

## Instructions for local testing

1. Running temporal locally

```shell
brew install temporal # To install
temporal server start-dev # To start the server locally
```

2. Run the integration test

Running the test directly from IntelliJ is the easiest but we can also run the following command
```shell
bazel run //orchestration:integration_tests_test_suite_src_test_scala_ai_chronon_orchestration_test_temporal_workflow_NodeExecutionWorkflowIntegrationSpec.scala
```

3. Go to the temporal UI on localhost for further debugging

`http://localhost:8233`



