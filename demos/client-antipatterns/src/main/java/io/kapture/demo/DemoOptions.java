package io.kapture.demo;

import java.util.Locale;

record DemoOptions(
    Scenario scenario,
    Mode mode,
    String broker,
    String topic,
    int count,
    String groupId) {

  enum Scenario {
    PRODUCER_LIFECYCLE("producer-lifecycle", "kapture-demo-producer-lifecycle", 12),
    OFFSET_COMMIT("offset-commit", "kapture-demo-offset-commit", 30);

    private final String cliName;
    private final String defaultTopic;
    private final int defaultCount;

    Scenario(String cliName, String defaultTopic, int defaultCount) {
      this.cliName = cliName;
      this.defaultTopic = defaultTopic;
      this.defaultCount = defaultCount;
    }

    String cliName() {
      return cliName;
    }

    String defaultTopic() {
      return defaultTopic;
    }

    int defaultCount() {
      return defaultCount;
    }

    static Scenario parse(String raw) {
      for (Scenario scenario : values()) {
        if (scenario.cliName.equals(raw)) {
          return scenario;
        }
      }
      throw new IllegalArgumentException(
          "unknown scenario '" + raw + "' (expected producer-lifecycle or offset-commit)");
    }
  }

  enum Mode {
    BAD,
    FIXED;

    static Mode parse(String raw) {
      try {
        return valueOf(raw.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException error) {
        throw new IllegalArgumentException(
            "unknown mode '" + raw + "' (expected bad or fixed)", error);
      }
    }

    String cliName() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  static DemoOptions parse(String[] args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("scenario and mode are required");
    }

    Scenario scenario = Scenario.parse(args[0]);
    Mode mode = Mode.parse(args[1]);
    String broker = "127.0.0.1:9092";
    String topic = scenario.defaultTopic();
    int count = scenario.defaultCount();
    String groupId = "kapture-demo-offset-commit-" + mode.cliName();

    for (int i = 2; i < args.length; i++) {
      String flag = args[i];
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("missing value after " + flag);
      }
      String value = args[++i];
      switch (flag) {
        case "--broker" -> broker = value;
        case "--topic" -> topic = value;
        case "--count" -> count = positiveInt(flag, value);
        case "--group" -> groupId = value;
        default -> throw new IllegalArgumentException("unknown option '" + flag + "'");
      }
    }

    return new DemoOptions(scenario, mode, broker, topic, count, groupId);
  }

  private static int positiveInt(String flag, String raw) {
    try {
      int value = Integer.parseInt(raw);
      if (value < 1) {
        throw new IllegalArgumentException(flag + " must be at least 1");
      }
      return value;
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException(flag + " expects an integer, got '" + raw + "'", error);
    }
  }
}
