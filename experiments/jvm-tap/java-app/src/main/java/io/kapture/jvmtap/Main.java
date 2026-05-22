package io.kapture.jvmtap;

/**
 * Single entry point so we ship one fat jar. Dispatches to
 * {@link Producer} or {@link Consumer} based on argv[0].
 */
public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: java -jar jvm-tap-app.jar <producer|consumer>");
      System.exit(2);
    }
    switch (args[0]) {
      case "producer" -> Producer.main(stripFirst(args));
      case "consumer" -> Consumer.main(stripFirst(args));
      default -> {
        System.err.println("Unknown mode: " + args[0] + " (expected producer|consumer)");
        System.exit(2);
      }
    }
  }

  private static String[] stripFirst(String[] args) {
    String[] rest = new String[args.length - 1];
    System.arraycopy(args, 1, rest, 0, rest.length);
    return rest;
  }
}
