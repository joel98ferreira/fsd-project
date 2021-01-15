package dkvs.server;

import java.nio.channels.AsynchronousChannelGroup;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class Main {

    /**
     * Main class for the Server to execute.
     * @param args Required to provide the args[0] that represents the path to the configuration for the server file.
     * @throws Exception Exception throw when creating BufferedReader or when reading line.
     */
    public static void main(String[] args) throws Exception {
        // If the user didn't provided the required file path, or provided more than one.
        if (args.length != 1){
            System.out.println("Required to provide config file path.");
            System.exit(2);
        }

        // Parse the configuration file (default path: ../configs/server-1.yaml)
        ServerConfig config = ServerConfig.parseServerConfigFileYaml(Paths.get(args[0]));

        // Create a thread group
        AsynchronousChannelGroup g =
                AsynchronousChannelGroup.withFixedThreadPool(20, defaultThreadFactory());

        // Start Server
        Server server = new Server(config);
        server.start(g);

        g.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        System.out.println("> Server Finished!");
    }
}
