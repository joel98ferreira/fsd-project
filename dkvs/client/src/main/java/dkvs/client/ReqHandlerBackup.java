package dkvs.client;


public class ReqHandlerBackup {
}
/*
public class RequestHandler {

    private static final Pattern COMMAND_PATTERN = Pattern.compile(
            "^\\s*(?<command>\\w*)(?<args>\\s*.*)$"
    );

    private final Client client;
    private final ConsistencyHash consistencyHash;
    private final BufferedReader in;

    public RequestHandler(Client client, ServersConfig serversConfig, BufferedReader in) {
        this.client = Objects.requireNonNull(client);
        this.consistencyHash = new ConsistencyHash(Objects.requireNonNull(serversConfig));
        this.in = Objects.requireNonNull(in);
    }

    public void inputLoop() throws IOException {
        while (true) {

            System.out.println("> ");

            // Read line from client input
            String line = this.in.readLine();

            if (line == null) {
                // Exited
                this.printWarning("Exiting...");
                break;
            }

            // Handle the received line
            if (!line.isEmpty())
                this.handleLine(line);
        }
    }

    private void handleLine(String line) {

        Matcher commandMatcher = COMMAND_PATTERN.matcher(line);

        // If the received line matches the regex
        if (commandMatcher.matches()) {

            String command = commandMatcher.group("command");
            String args = commandMatcher.group("args");

            switch (command) {
                case "get":
                    this.handleGet(args);
                    break;

                case "put":
                    this.handlePut(args);
                    break;

                default:
                    this.printError("Unknown command, must be 'get', 'put'.");
                    break;
            }
        }
    }

    private void handleGet(String keys) {
        // get chirps

        final List< String > chirps;

        try
        {
            chirps = this.client.getLatestChirps();
        }
        catch (Exception e)
        {
            this.printError(e.getMessage());
            return;
        }

        // print chirps

        if (chirps.isEmpty())
        {
            this.printWarning(
                    this.client.getSubscribedTopics().isEmpty()
                            ? "No chirps exist."
                            : "No chirps exist for any of your subscribed topics."
            );
        }
        else
        {
            for (final var chirp : chirps)
                this.out.println(chirp);

            this.out.flush();
        }
    }

    private void handlePut(String topics) {
        try
        {
            this.client.setSubscribedTopics(
                    Pattern
                            .compile("\\s+")
                            .splitAsStream(topics)
                            .filter(t -> !t.isBlank())
                            .collect(Collectors.toList())
            );
        }
        catch (IllegalArgumentException e)
        {
            this.printError(e.getMessage());
        }
    }

    private void printWarning(String warning)
    {
        System.out.printf("\033[33m%s\033[0m\n", warning);
    }

    private void printError(String error) {
        System.out.printf("\033[31m%s\033[0m\n", error);
    }
}
*/
