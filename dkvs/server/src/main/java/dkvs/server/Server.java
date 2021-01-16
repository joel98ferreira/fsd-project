package dkvs.server;

import dkvs.server.identity.*;

import dkvs.server.network.ServerNetwork;
import spullara.nio.channels.FutureServerSocketChannel;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;

public class Server {

    private final ServerId serverId;
    private final ServerAddress serverAddress;
    private final KeyValueStore keyValueStore;
    private final RequestHandler requestHandler;
    private final ServerNetwork serverNetwork;

   public Server(ServerConfig config) {
       this.serverId = config.getLocalServerId();
       this.serverAddress = config.getLocalServerAddress();
       this.keyValueStore = new KeyValueStore();
       this.serverNetwork = new ServerNetwork(config);
       this.requestHandler = new RequestHandler(config, serverNetwork, keyValueStore);

       // NA NETWORK TER UMA CENA PARA RECEIVE DO
   }

   public void start(AsynchronousChannelGroup g) throws IOException, InterruptedException {
       // Open a socket channel with a thread pool group
       FutureServerSocketChannel server = FutureServerSocketChannel.open(g);

       // Bind the server to listen for connections
       server.bind(serverAddress.getSocketAddress());

       // Start accepting new clients
       new ClientAcceptor(this.requestHandler).acceptNew(serverAddress, server);

       // Start the server network
       this.serverNetwork.start(g, requestHandler);
   }

    public ServerId getServerId() {
        return serverId;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

    public RequestHandler getRequestHandler() {
        return requestHandler;
    }

    public ServerNetwork getServerNetwork() {
        return serverNetwork;
    }
}
