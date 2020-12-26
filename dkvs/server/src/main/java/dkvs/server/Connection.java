package dkvs.server;

import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class Connection {

    public static void acceptNew(FutureServerSocketChannel server, State state){
        CompletableFuture<FutureSocketChannel> futureClient = server.accept();

        futureClient.thenAccept(client -> {
            state.clients.add(new Client(client, state.messages.currentId()));
            System.out.println("Accepted [" + state.clients.connected() + " clients connected].");
            ByteBuffer buf = ByteBuffer.allocate(16);
            ClientConnection.read(client, state, buf);
            Connection.acceptNew(server, state);
        });

    }
}
