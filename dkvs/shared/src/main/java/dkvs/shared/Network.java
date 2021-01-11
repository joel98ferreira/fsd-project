package dkvs.shared;

import spullara.nio.channels.FutureSocketChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class Network {

    private final FutureSocketChannel socket;
    private ByteBuffer byteBuffer;
    private final Serializer serializer;

    public Network(FutureSocketChannel socket, ByteBuffer byteBuffer) {
        this.socket = Objects.requireNonNull(socket);
        this.byteBuffer = Objects.requireNonNull(byteBuffer);
        this.serializer = new Serializer();
    }

    public CompletableFuture<Message> receive() {
        CompletableFuture<Message> acceptor = new CompletableFuture<>();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        socket.read(byteBuffer).thenAccept(rd -> {
            if (rd == -1) {
                acceptor.complete(null);
                return;
            }

            byteBuffer.flip();

            while (byteBuffer.hasRemaining()) {
                byte b = byteBuffer.get();
                bytes.write(b);
            }

            try {
                byteBuffer.clear();
                acceptor.complete(serializer.deserialize(bytes.toByteArray()));
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Error while deserializing.");
                e.printStackTrace();
            }
        });

        return acceptor;
    }

    private void sendRecursive(CompletableFuture<Void> acceptor) {
        socket.write(byteBuffer).thenAccept(wr -> {
            if(byteBuffer.hasRemaining()) {
                sendRecursive(acceptor);
            } else {
               acceptor.complete(null);
               byteBuffer.clear();
            }
        });
    }

    public CompletableFuture<Void> send(Message message) throws IOException {

        // Serialize the request
        byteBuffer = ByteBuffer.wrap(serializer.serialize(message));

        CompletableFuture<Void> acceptor = new CompletableFuture<>();

        // Send until no bytes remain in the buffer
        sendRecursive(acceptor);

        return acceptor;
    }

    public CompletableFuture<Message> sendAndReceive(Message message) throws IOException {
        CompletableFuture<Message> response = new CompletableFuture<>();

        send(message).thenAccept(v -> receive().thenAccept(response::complete));

        return response;
    }

    public void close(){
        // Close the socket
        this.socket.close();
        this.byteBuffer.clear();
    }
}