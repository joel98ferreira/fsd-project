package dkvs.client;

import dkvs.server.identity.ServerAddress;

import java.io.*;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        // Create a thread group to handle receives
        AsynchronousChannelGroup g =
                AsynchronousChannelGroup.withFixedThreadPool(1, defaultThreadFactory());

        // Parse the server address from the args
        ServerAddress serverAddress = parseServerAddress(args);

        // Create a new Client
        Client client = new Client(serverAddress);

        // Start the client and connect to server
        client.start(g);

        // Create a request handler
        RequestHandler requestHandler = new RequestHandler(client, new BufferedReader(new InputStreamReader(System.in)));

        // Execute the input loop
        requestHandler.inputLoop();

        g.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        System.out.println("> Client Finished!");
    }

    private static ServerAddress parseServerAddress(String[] args){

        if (args.length != 2){
            System.out.println("Required to provide host and server port.");
            System.exit(2);
        }

        return new ServerAddress(args[0], Integer.parseInt(args[1]));
    }
}
