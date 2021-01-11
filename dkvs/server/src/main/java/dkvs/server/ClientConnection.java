package dkvs.server;

import dkvs.shared.*;

import spullara.nio.channels.FutureSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ClientConnection {

    private final String clientUUID;
    private final Network network;
    private final RequestHandler requestHandler;

    public ClientConnection(String clientUUID, FutureSocketChannel socketChannel, ByteBuffer byteBuffer, RequestHandler requestHandler) {
        this.clientUUID = clientUUID;
        this.network = new Network(Objects.requireNonNull(socketChannel), Objects.requireNonNull(byteBuffer));
        this.requestHandler = Objects.requireNonNull(requestHandler);
    }

    public void read() {
        network.receive().thenAccept(message -> {
            if(message == null){
                // Close the connection
                network.close();
                System.out.println("> Connection closed");
                return;
            }

            // Handle the received message
            this.requestHandler.handleMessage(message, clientUUID, network);

            // Keep reading for client requests
            this.read();
        });
    }


    public void write(RequestType type, Object object) throws IOException {

        Message message = new Message(UUID.randomUUID().toString(), type, object);

       CompletableFuture<Void> writer = network.send(message);

       writer.thenAccept(vd -> {
           System.out.println("Sent to " + network.toString());
            //client.messageId++;
            //write();
      });
    }
}