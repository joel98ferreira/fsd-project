package dkvs.server;

import dkvs.server.identity.*;

import dkvs.server.network.ServerNetwork;
import spullara.nio.channels.FutureServerSocketChannel;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.defaultThreadFactory;

public class Server {

    private final ServerId serverId;
    private final ServerAddress serverAddress;
    private final KeyValueStore keyValueStore;
    private final ServerNetwork connectedServers;

   public Server(ServerConfig config){
       this.serverId = config.getLocalServerId();
       this.serverAddress = config.getLocalServerAddress();
       this.keyValueStore = new KeyValueStore();
       this.connectedServers = new ServerNetwork(config);

       // NA NETWORK TER UMA CENA PARA RECEIVE DO
   }

   public void start() throws IOException, InterruptedException {
       //State state = new State();
       AsynchronousChannelGroup g =
               AsynchronousChannelGroup.withFixedThreadPool(1, defaultThreadFactory());

       // Open a socket channel with a thread pool group
       FutureServerSocketChannel server = FutureServerSocketChannel.open(g);

       // Bind the server to listen for connections
       server.bind(serverAddress.getSocketAddress());

       // Start accepting new clients
       Connection.acceptNew(server);

       g.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
       System.out.println("> Finished!");
   }
}
