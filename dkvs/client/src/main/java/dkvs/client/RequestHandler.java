package dkvs.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RequestHandler {

    private final Client client;
    private final RequestState requestState;

    private static final Pattern COMMAND_PATTERN = Pattern.compile(
            "^\\s*(?<command>\\w*)(?<args>\\s*.*)$"
    );

    private final BufferedReader in;

    public RequestHandler(Client client, BufferedReader in) {
        this.client = Objects.requireNonNull(client);
        this.requestState = new RequestState();
        this.in = in;
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

    private void handleLine(String line) throws IOException {

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

    private void handleGet(String keys) throws IOException {

        // Split all input by spaces
        Collection<String> keysList = Pattern.compile("\\s+").
                splitAsStream(keys).
                filter(k -> !k.isBlank()).
                collect(Collectors.toList());

        Collection<Long> keysLong = new ArrayList<>();

        for (String key : keysList){
            keysLong.add(Long.parseLong(key));
        }

        System.out.println("> Sending to the server: ");
        for (Long key : keysLong){
            System.out.println("\t-> Get on key: " + key);
        }

        CompletableFuture<Map<Long, byte[]>> req = this.client.get(keysLong);

        req.thenAccept(values -> {
            for (Long k : values.keySet()){
                System.out.println("> Get successful on key: " + k + ", " + new String(values.get(k)));
            }
        });
    }

    private void handlePut(String keysValuesString) {
        try {
            // Accepted input: 1->hello 2->distributed 3->systems 4->world
            Collection<String> keysVals = Pattern.compile("\\s+").
                    splitAsStream(keysValuesString).
                    filter(k -> !k.isBlank()).
                    collect(Collectors.toList());

            Map <Long, byte[]> keyValues = new HashMap<>();

            Pattern keyValPattern = Pattern.compile("\\d+->\\w+");

            for (String keyValStr : keysVals){
                Matcher keyValMatcher = keyValPattern.matcher(keyValStr);

                if (!keyValMatcher.matches()){
                    this.printError("Invalid syntax!\nUsage (no spaces between key->val): put k->v k->v k->v");
                    return;
                }

                String keyVal = keyValStr.replace(">", "");
                String[] kv_array = keyVal.split("-");

                keyValues.put(Long.parseLong(kv_array[0]), kv_array[1].getBytes(StandardCharsets.UTF_8));
            }

            if (keyValues.size() == 0){
                this.printError("Invalid!\nUsage (no spaces between key->val): put k->v k->v k->v");
            } else {

                System.out.println("> Sending to the server: ");
                for (Long key : keyValues.keySet()){
                    System.out.println("\t-> Put on key: " + key);
                }

                CompletableFuture<Void> req = this.client.put(keyValues);

                req.thenAccept(n -> {
                    System.out.println("Put successful.");
                });
            }

        } catch (IllegalArgumentException | IOException e) {
            this.printError(e.getMessage());
        }
    }

    private void printWarning(String warning) {
        System.out.printf("\033[33m%s\033[0m\n", warning);
    }

    private void printError(String error) {
        System.out.printf("\033[31m%s\033[0m\n", error);
    }
}
