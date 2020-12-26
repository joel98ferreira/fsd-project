package dkvs.server;

import spullara.nio.channels.FutureServerSocketChannel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class Main {

    /**
     * Main class for the Server to execute.
     * @param args Required to provide the args[0] that represents the path to the configuration for the server file.
     * @throws Exception Exception throw when creating BufferedReader or when reading line.
     */
    public static void main(String[] args) throws Exception{
        try(
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
        ){
            // If the user didn't provided the required file path, or provided more than one.
            if (args.length != 1){
                out.println("Required to provide config file path.\nUsage: dkvs-server <config-file-path>");
                System.exit(2);
            }

            // Parse the configuration file
            //ServerConfig config = ServerConfig.parseYamlFile(Path.of(args[0]));

            State state = new State();
            AsynchronousChannelGroup g =
                    AsynchronousChannelGroup.withFixedThreadPool(1, defaultThreadFactory());

            FutureServerSocketChannel server =
                    new FutureServerSocketChannel();
            server.bind(new InetSocketAddress(12345));

            Connection.acceptNew(server, state);

        }

        //in.readLine();
        //out.println(2);
        //out.flush();
    }
}
