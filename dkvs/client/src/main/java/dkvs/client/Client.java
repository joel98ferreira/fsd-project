package dkvs.client;

import dkvs.server.identity.ServerAddress;
import dkvs.shared.*;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Client {

    // The address of the server that this client is connected to
    private final ServerAddress serverAddress;

    // The network connecting the client and the server
    private Network network;

    public Client(ServerAddress serverAddress) {
        this.serverAddress = Objects.requireNonNull(serverAddress);
    }

    public void start(AsynchronousChannelGroup g) throws IOException {

        // Connect to the server
        Connection.connect(serverAddress.getSocketAddress(), g).thenAccept(network -> {
            System.out.println("> Connected to server " + serverAddress.getSocketAddress().toString() + "!");

            this.network = Objects.requireNonNull(network);

            // Start the network
            this.network.start();

            System.out.println("> Client can now make requests.");
            System.out.println("Usage examples:\n\tput 1->hello 2->distributed 3->systems 4->world\n\tget 1 2 3 4");
        });
    }

    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys) throws IOException {

        CompletableFuture<Map<Long, byte[]>> response = new CompletableFuture<>();

        // Encapsulate the request in a message
        Message request = new Message(new MessageId(), RequestType.GET_REQUEST, keys);

        this.network.send(request).thenAccept(v -> {
            System.out.println("> Send the Get request to the server.");
        });

        return response;
    }

    // TODO: Adicionar queue de mensagens e depois reenviar se não receber resposta num timeout definido

    public CompletableFuture<Void> put(Map<Long, byte[]> values) throws IOException {

        CompletableFuture<Void> response = new CompletableFuture<>();

        // Encapsulate the request in a message
        Message request = new Message(new MessageId(), RequestType.PUT_REQUEST, values);

        this.network.send(request).thenAccept(v -> {
            System.out.println("> Send the Put request to the server.");
        }).thenCompose(r -> read());



        return response;
    }

    public CompletableFuture<Message> read() {
        CompletableFuture<Message> acceptor = new CompletableFuture<>();

        this.network.receive().thenAccept(message -> {
            if(message == null){
                // Close the connection
                network.close();
                System.out.println("> Connection closed");
                return;
            }


            System.out.println("> Received: " + message.getType());

            acceptor.complete(message);
        }).thenCompose(r -> read());

        return acceptor;
    }

    /*
            this.network.receive().thenAccept(message -> {
            System.out.println("> Received the Put response!");
            System.out.println(message.getType());
            if (message.getType() == RequestType.PUT_REPLY){
                System.out.println("> Put executed successfully");
                response.complete(null);
            }
        });

                    this.network.receive().thenAccept(message -> {
                System.out.println("> Received the Get response!");
                if (message.getType() == RequestType.GET_REPLY){
                    Map<Long, byte[]> map = (Map<Long, byte[]>) message.getContent();
                    response.complete(map);
                }
            });
     */
}
