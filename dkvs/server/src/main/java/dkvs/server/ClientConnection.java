package dkvs.server;

import dkvs.server.identity.ClientId;
import dkvs.server.network.ServerNetwork;
import dkvs.shared.*;

import dkvs.shared.Connection;
import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class ClientConnection {

    private final ClientId clientId;
    private final Network network;
    private final RequestHandler requestHandler;

    public ClientConnection(ClientId clientId, FutureSocketChannel socketChannel, ByteBuffer byteBuffer, RequestHandler requestHandler) {
        this.clientId = Objects.requireNonNull(clientId);
        this.network = new Network(Objects.requireNonNull(socketChannel), Objects.requireNonNull(byteBuffer));

        // Register the default payloads and the server payloads and start the network
        Connection.registerNetworkDefaultPayloads(network);
        ServerNetwork.registerServerPayloadsAndStartNetwork(network);

        this.requestHandler = Objects.requireNonNull(requestHandler);
    }

    public CompletableFuture<Message> read() {
        CompletableFuture<Message> acceptor = new CompletableFuture<>();

        network.receive().thenAccept(message -> {
            if(message == null){
                // Close the connection
                network.close();
                System.out.println("> Connection closed");
                return;
            }

            // Handle the received message
            this.requestHandler.handleMessage(message, this.clientId, network);

            acceptor.complete(message);
        }).thenCompose(r -> read());

        return acceptor;
    }

    public void write(RequestType type, Object object) throws IOException {

        Message message = new Message(new MessageId(), type, object);

       CompletableFuture<Void> writer = network.send(message);

       writer.thenAccept(vd -> {
           System.out.println("Sent to " + network.toString());
            //client.messageId++;
            //write();
      });
    }
}