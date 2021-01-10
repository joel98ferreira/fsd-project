package dkvs.server;

import dkvs.server.identity.ServerAddress;
import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Connection {

    private final RequestHandler requestHandler;

    public Connection(RequestHandler requestHandler) {
        this.requestHandler = Objects.requireNonNull(requestHandler);
    }

    public void acceptNew(ServerAddress address, FutureServerSocketChannel server) {

        System.out.println("> Server is listening for connections at " + address.getPort());
        CompletableFuture<FutureSocketChannel> futureClient = server.accept();

        futureClient.thenAccept(client -> {
            System.out.println("> New client connected.");

            ByteBuffer buf = ByteBuffer.allocate(1024);

            ClientConnection con = new ClientConnection(UUID.randomUUID().toString(), client, buf, requestHandler);

            // Start receiving from client input
            con.read();
            acceptNew(address, server);
        });
    }

}
