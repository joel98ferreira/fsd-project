package dkvs.server;

import spullara.nio.channels.FutureSocketChannel;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class FutureSocketChannelWriter {

    static private void writeUntilNoneRemaining(FutureSocketChannel socket
            , ByteBuffer buf
            , CompletableFuture<Void> acceptor){
        socket.write(buf).thenAccept(wr -> {
            if(buf.hasRemaining())
                writeUntilNoneRemaining(socket, buf, acceptor);
            else
                acceptor.complete(null);
        });
    }

    static public CompletableFuture<Void> write(FutureSocketChannel socket, String str){
        CompletableFuture<Void> acceptor = new CompletableFuture<>();
        ByteBuffer buf = ByteBuffer.wrap(str.getBytes());

        writeUntilNoneRemaining(socket, buf, acceptor);

        return acceptor;
    }
}
