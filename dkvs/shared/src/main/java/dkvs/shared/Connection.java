package dkvs.shared;

import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class Connection {

    public static CompletableFuture<Network> connect(SocketAddress address) throws IOException {

        CompletableFuture<Network> connect = new CompletableFuture<>();
        FutureSocketChannel socket = new FutureSocketChannel();

        socket.connect(address).thenAccept(c -> {

            // Allocate a byte buffer
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

            // Create a new network with the socket to the server and the byte buffer allocated
            Network network = new Network(socket, byteBuffer);

            connect.complete(network);
        });

        return connect;
    }
}