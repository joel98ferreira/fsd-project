package dkvs.client;

import dkvs.server.identity.ServerAddress;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws IOException{

        // Parse the server address from the args
        ServerAddress serverAddress = parseServerAddress(args);

        // Create a new Client
        Client client = new Client(serverAddress);

        // Start the client and connect to server
        client.start();

        // Create a request handler
        RequestHandler requestHandler = new RequestHandler(client);

        // Execute tests
        HashMap<Long, byte[]> r = new HashMap<>();
        r.put(1L, "o".getBytes(StandardCharsets.UTF_8));
        r.put(2L, "l".getBytes(StandardCharsets.UTF_8));
        r.put(3L, "a".getBytes(StandardCharsets.UTF_8));
        requestHandler.handlePut(r);

        // Block to wait for server response
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        in.readLine();
    }

    private static ServerAddress parseServerAddress(String[] args){

        if (args.length != 2){
            System.out.println("Required to provide host and server port.");
            System.exit(2);
        }

        return new ServerAddress(args[0], Integer.parseInt(args[1]));
    }
}
