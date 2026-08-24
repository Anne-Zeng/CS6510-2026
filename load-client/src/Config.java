import java.util.HashMap;
import java.util.Map;

/** Parses --key=value command-line arguments into a typed configuration. */
public final class Config {

    public final String baseUrl;
    public final int stations;
    public final int durationSeconds;
    public final int minItems;
    public final int maxItems;
    public final int popularLimit;
    public final int requestTimeoutSeconds;
    public final boolean verbose;
    public final String reportDir;

    private Config(Builder b) {
        this.baseUrl = b.baseUrl;
        this.stations = b.stations;
        this.durationSeconds = b.durationSeconds;
        this.minItems = b.minItems;
        this.maxItems = b.maxItems;
        this.popularLimit = b.popularLimit;
        this.requestTimeoutSeconds = b.requestTimeoutSeconds;
        this.verbose = b.verbose;
        this.reportDir = b.reportDir;
    }

    public static Config parse(String[] args) {
        Map<String, String> raw = new HashMap<>();
        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                printHelpAndExit();
            }
            if (!arg.startsWith("--") || !arg.contains("=")) {
                System.err.println("Ignoring unrecognized argument: " + arg);
                continue;
            }
            String[] parts = arg.substring(2).split("=", 2);
            raw.put(parts[0], parts[1]);
        }

        Builder b = new Builder();
        b.baseUrl = raw.getOrDefault("baseUrl", "http://localhost:8080");
        b.stations = Integer.parseInt(raw.getOrDefault("stations", "10"));
        b.durationSeconds = Integer.parseInt(raw.getOrDefault("duration", "60"));
        b.minItems = Integer.parseInt(raw.getOrDefault("minItems", "1"));
        b.maxItems = Integer.parseInt(raw.getOrDefault("maxItems", "20"));
        b.popularLimit = Integer.parseInt(raw.getOrDefault("popularLimit", "10"));
        b.requestTimeoutSeconds = Integer.parseInt(raw.getOrDefault("requestTimeout", "10"));
        b.verbose = Boolean.parseBoolean(raw.getOrDefault("verbose", "false"));
        b.reportDir = raw.getOrDefault("reportDir", "./reports");

        if (b.minItems < 1 || b.maxItems < b.minItems) {
            throw new IllegalArgumentException("Require 1 <= minItems <= maxItems");
        }
        return new Config(b);
    }

    private static void printHelpAndExit() {
        System.out.println("""
                Self-Checkout Load Client

                Usage: java Main [--key=value ...]

                Options:
                  --baseUrl=URL          Base URL of the system under test (default: http://localhost:8080)
                  --stations=N           Number of concurrent simulated checkout stations (default: 10)
                  --duration=SECONDS     How long to run the test, in seconds (default: 60)
                  --minItems=N           Minimum items per basket (default: 1)
                  --maxItems=N           Maximum items per basket (default: 20)
                  --popularLimit=N       How many popular items to request at the end (default: 10)
                  --requestTimeout=SECS  Per-request timeout in seconds (default: 10)
                  --verbose=true|false   Print each transaction as it completes (default: false)
                  --reportDir=PATH       Where to write the JSON report (default: ./reports)

                Examples:
                  java Main
                  java Main --stations=10 --duration=120
                  java Main --stations=200 --duration=180 --reportDir=./reports/stress
                """);
        System.exit(0);
    }

    private static final class Builder {
        String baseUrl;
        int stations;
        int durationSeconds;
        int minItems;
        int maxItems;
        int popularLimit;
        int requestTimeoutSeconds;
        boolean verbose;
        String reportDir;
    }
}
