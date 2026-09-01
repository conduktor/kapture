package io.kapture.demo;

public final class Main {
  private Main() {}

  public static void main(String[] args) {
    // Keep the terminal legible on stage. Kapture is where the protocol
    // detail belongs; this process only narrates the application calls.
    System.getProperties().putIfAbsent("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    System.getProperties().putIfAbsent("org.slf4j.simpleLogger.log.org.apache.kafka", "error");

    try {
      if (args.length > 0 && "setup".equals(args[0])) {
        TopicSetup.run(args);
        return;
      }
      DemoOptions options = DemoOptions.parse(args);
      printHeader(options);
      switch (options.scenario()) {
        case PRODUCER_LIFECYCLE -> ProducerLifecycleScenario.run(options);
        case OFFSET_COMMIT -> OffsetCommitScenario.run(options);
      }
    } catch (IllegalArgumentException error) {
      System.err.println("error: " + error.getMessage());
      System.err.println();
      printUsage();
      System.exit(2);
    } catch (Exception error) {
      System.err.println("demo failed: " + error.getMessage());
      error.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void printHeader(DemoOptions options) {
    System.out.println("Kapture real-client anti-pattern harness");
    System.out.println("scenario : " + options.scenario().cliName());
    System.out.println("mode     : " + options.mode().cliName());
    System.out.println("broker   : " + options.broker());
    System.out.println("topic    : " + options.topic());
    System.out.println("count    : " + options.count());
    if (options.scenario() == DemoOptions.Scenario.OFFSET_COMMIT) {
      System.out.println("group    : " + options.groupId());
    }
    System.out.println();
  }

  private static void printUsage() {
    System.err.println("Usage:");
    System.err.println("  java -jar target/kapture-client-antipatterns.jar setup [--broker host:port]");
    System.err.println("  java -jar target/kapture-client-antipatterns.jar \\");
    System.err.println("    <producer-lifecycle|offset-commit> <bad|fixed> [options]");
    System.err.println();
    System.err.println("Options:");
    System.err.println("  --broker <host:port>  Kafka bootstrap (default: 127.0.0.1:9092)");
    System.err.println("  --topic <name>        Override the scenario topic");
    System.err.println("  --count <n>           Records to process (defaults: 12 / 30)");
    System.err.println("  --group <id>          Consumer group for offset-commit");
  }
}
