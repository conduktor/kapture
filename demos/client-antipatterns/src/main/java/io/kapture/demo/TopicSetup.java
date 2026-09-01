package io.kapture.demo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

final class TopicSetup {
  private static final List<String> DEFAULT_TOPICS = List.of(
      DemoOptions.Scenario.PRODUCER_LIFECYCLE.defaultTopic(),
      DemoOptions.Scenario.OFFSET_COMMIT.defaultTopic());

  private TopicSetup() {}

  static void run(String[] args) throws Exception {
    String broker = "localhost:29092";
    for (int i = 1; i < args.length; i++) {
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("missing value after " + args[i]);
      }
      String flag = args[i];
      String value = args[++i];
      if ("--broker".equals(flag)) {
        broker = value;
      } else {
        throw new IllegalArgumentException("unknown setup option '" + flag + "'");
      }
    }

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kapture-demo-setup");
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

    System.out.println("Preparing demo topics on " + broker);
    try (Admin admin = Admin.create(props)) {
      for (String topic : DEFAULT_TOPICS) {
        try {
          admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
          System.out.println("  created " + topic);
        } catch (ExecutionException error) {
          if (error.getCause() instanceof TopicExistsException) {
            System.out.println("  exists  " + topic);
          } else {
            throw error;
          }
        }
      }
    }
  }
}
