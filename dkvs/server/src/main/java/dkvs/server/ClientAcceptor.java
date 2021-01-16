package dkvs.server;

import dkvs.server.identity.ClientId;
import dkvs.server.identity.ServerAddress;
import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class ClientAcceptor {

    private final RequestHandler requestHandler;

    public ClientAcceptor(RequestHandler requestHandler) {
        this.requestHandler = Objects.requireNonNull(requestHandler);
    }

    public void acceptNew(ServerAddress address, FutureServerSocketChannel server) {

        System.out.println("> Server is listening for connections at " + address.getPort());
        CompletableFuture<FutureSocketChannel> futureClient = server.accept();

        futureClient.thenAccept(client -> {

            // Generate a new client UUID
            ClientId clientUUID = new ClientId();

            System.out.println("> New client connected.\n> Client Id: " + clientUUID);

            ByteBuffer buf = ByteBuffer.allocate(1024);

            ClientConnection clientConnection = new ClientConnection(clientUUID, client, buf, requestHandler);

            // Accept new connections
            acceptNew(address, server);

            // Start receiving from client input
            clientConnection.read();
        });
    }

}
