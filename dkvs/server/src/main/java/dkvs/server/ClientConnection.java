package dkvs.server;

import dkvs.shared.*;

import spullara.nio.channels.FutureSocketChannel;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class ClientConnection {

    private final Network network;

    public ClientConnection(FutureSocketChannel socketChannel, ByteBuffer byteBuffer) {
        this.network = new Network(Objects.requireNonNull(socketChannel),  Objects.requireNonNull(byteBuffer));
    }

    public void read() {
        network.receive().thenAccept(message -> {
            if(message == null){
                // Close the connection
                network.close();
                System.out.println("Connection closed");
                return;
            }
            System.out.println("Received : " + message.getType());

            // Keep reading for client input
            this.read();
        });
    }


    public void write(){

        CompletableFuture<Void> writer = network.send(client.socket, message);

        writter.thenAccept(vd -> {
            System.out.println("Wrote " + message + " bytes");
            client.messageId++;
            write(client);
        });
    }
}