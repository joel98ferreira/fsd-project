package dkvs.server;

import spullara.nio.channels.FutureSocketChannel;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

public class FutureSocketChannelReader {

    private static boolean readLineInto(ByteBuffer buf, ByteArrayOutputStream bytes) {
        boolean ret = false;
        while (buf.hasRemaining() && !ret) {
            byte b = buf.get();
            bytes.write(b);
            ret = b == '\n';
        }
        buf.compact();
        return ret;
    }

    public static CompletableFuture<String> readLine(FutureSocketChannel socket, ByteBuffer buf) {
        CompletableFuture<String> acceptor = new CompletableFuture<>();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        boolean foundLine = false;
        if (buf.position() > 0) {
            buf.flip();
            foundLine = readLineInto(buf, bytes);
        }
        if (foundLine)
            acceptor.complete(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
        else {
            socket.read(buf).thenAccept(rd -> {
                if (rd == -1) {
                    acceptor.complete(null);
                    return;
                }
                buf.flip();
                readLineInto(buf, bytes);
                acceptor.complete(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
            });
        }

        return acceptor;
    }
}
