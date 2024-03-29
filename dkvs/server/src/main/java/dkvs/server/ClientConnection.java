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
                requestHandler.removeConnection(clientId);
                return;
            }

            // Handle the received message
            try {
                this.requestHandler.handleMessage(message, this.clientId, network);
            } catch (IOException e) {
                System.out.println("> Error while handling the message.");
            }

            acceptor.complete(message);
        }).thenCompose(r -> read());

        return acceptor;
    }
}