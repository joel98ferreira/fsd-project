package dkvs.shared;

import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import spullara.nio.channels.FutureSocketChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class Network {

    private final FutureSocketChannel socket;
    private ByteBuffer byteBuffer;
    private Serializer serializer;
    private final SerializerBuilder serializerBuilder;

    public Network(FutureSocketChannel socket, ByteBuffer byteBuffer) {
        this.socket = Objects.requireNonNull(socket);
        this.byteBuffer = Objects.requireNonNull(byteBuffer);
        this.serializerBuilder = Serializer.builder();
    }

    /**
     * Once we have all the payload types registered start the serializer.
     */
    public void start(){
        this.serializer = this.serializerBuilder.build();
    }

    /**
     * Method to register a determined payload type class.
     * @param payloadClass The payload class.
     */
    public void registerPayloadType(Class<?> payloadClass){
        this.serializerBuilder.addType(payloadClass);
    }

    /**
     * Method that starts receiving in the socket recursively and returns a completable future
     * when it receives something in the socket.
     * @return A message that in the future will be received.
     */
    public CompletableFuture<Message> receive() {
        CompletableFuture<Message> acceptor = new CompletableFuture<>();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        byteBuffer.clear();

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

            byteBuffer.clear();

            acceptor.complete(serializer.decode(bytes.toByteArray()));
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

    /**
     * Method that sends a message to the socket in this network, it returns a completable
     * future. This is only completed when we are sure that no bytes are left in the buffer.
     * @param message The message to send.
     */
    public CompletableFuture<Void> send(Message message) throws IOException {

        // Serialize the request
        byteBuffer = ByteBuffer.wrap(serializer.encode(message));

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

    /**
     * Close the network.
     */
    public void close(){
        // Close the socket
        this.socket.close();
        this.byteBuffer.clear();
    }
}