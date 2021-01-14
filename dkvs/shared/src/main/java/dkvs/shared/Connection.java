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

            // Register the default payloads
            registerNetworkDefaultPayloads(network);

            connect.complete(network);
        });

        return connect;
    }

    /**
     * Method that register in a network all of the default payloads.
     */
    public static void registerNetworkDefaultPayloads(Network network){
        network.registerPayloadType(Message.class);
        network.registerPayloadType(MessageId.class);
        network.registerPayloadType(RequestType.class);
    }
}