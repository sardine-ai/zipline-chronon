# Chronon PubSub Module

This module provides a flexible, modular, and lightweight abstraction for working with Google Cloud Pub/Sub.

## Components

The PubSub module is organized into several components to separate concerns and promote flexibility:

### 1. Messages (`PubSubMessage.scala`)

- `PubSubMessage` - Base trait for all messages that can be published to PubSub
- `JobSubmissionMessage` - Implementation for job submission messages

### 2. Configuration (`PubSubConfig.scala`)

- `PubSubConfig` - Configuration for PubSub connections
- Helper methods for creating production and emulator configurations

### 3. Admin (`PubSubAdmin.scala`)

- `PubSubAdmin` - Interface for managing topics and subscriptions
- `GcpPubSubAdmin` - Implementation for Google Cloud Pub/Sub

### 4. Publisher (`PubSubPublisher.scala`)

- `PubSubPublisher` - Interface for publishing messages
- `GcpPubSubPublisher` - Implementation for Google Cloud Pub/Sub

### 5. Subscriber (`PubSubSubscriber.scala`)

- `PubSubSubscriber` - Interface for receiving messages
- `GcpPubSubSubscriber` - Implementation for Google Cloud Pub/Sub

### 6. Manager (`PubSubManager.scala`)

- `PubSubManager` - Manages PubSub components and provides caching
- Factory methods for creating configured managers

## Usage Examples

### Basic Production Usage

```scala
// Create a manager for production
val manager = PubSubManager.forProduction("my-project-id")

// Get a publisher
val publisher = manager.getOrCreatePublisher("my-topic")

// Create and publish a message
val message = JobSubmissionMessage("my-node", Some("Job data"))
val future = publisher.publish(message)

// Get a subscriber
val subscriber = manager.getOrCreateSubscriber("my-topic", "my-subscription")

// Pull messages
val messages = subscriber.pullMessages(10)

// Remember to shutdown when done
manager.shutdown()
```

### Testing with Emulator

```scala
// Create a manager for the emulator
val manager = PubSubManager.forEmulator("test-project", "localhost:8085")

// Now use it the same way as production
val publisher = manager.getOrCreatePublisher("test-topic")
val subscriber = manager.getOrCreateSubscriber("test-topic", "test-subscription")
```

### Integration with NodeExecutionActivity

```scala
// Create a publisher for the activity
val publisher = PubSubManager.forProduction("my-project-id")
  .getOrCreatePublisher("job-submissions")

// Create the activity with the publisher
val activity = NodeExecutionActivityFactory.create(workflowClient, publisher)
```

## Benefits

1. **Separation of Concerns** - Each component has a single responsibility
2. **Dependency Injection** - Easy to inject and mock for testing
3. **Caching** - Publishers and subscribers are cached for efficiency
4. **Resource Management** - Clean shutdown of all resources
5. **Emulator Support** - Seamless support for local testing with emulator