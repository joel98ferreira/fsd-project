package dkvs.server;

import spullara.nio.channels.FutureSocketChannel;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ClientConnection {

    public static void read(FutureSocketChannel client
            , State state
            , ByteBuffer buf) {
        FutureSocketChannelReader.readLine(client, buf).thenAccept(message -> {
            if(message == null){
                state.clients.remove(client);
                System.out.println("Connection closed");
                return;
            }
            System.out.println("Received : " + message);
            state.messages.addMessage(message);
            ClientConnection.handleWrites(state);
            ClientConnection.read(client, state, buf);
        });
    }

    public static void handleWrites(State state){
        int id = state.messages.currentId();
        Client client;
        while((client = state.clients.handle(id)) != null) {
            write(client, state);
        }
    }

    public static void write(Client client, State state){
        String message = state.messages.getNextMessage(client.messageId);
        if(message == null){
            state.clients.handled(client);
            return;
        }

        CompletableFuture<Void> writter = FutureSocketChannelWriter.write(client.socket, message);

        writter.thenAccept(vd -> {
            //System.out.println("Wrote " + message + " bytes");
            client.messageId++;
            write(client, state);
        });
    }

}