package dkvs.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RequestHandler {

    private final Client client;

    public RequestHandler(Client client) {
        this.client = Objects.requireNonNull(client);
    }

    public void handleGet(Collection<Long> keys) throws IOException {

        CompletableFuture<Map<Long, byte[]>> req = this.client.get(keys);

        req.thenAccept(values -> {
           for (Long k : values.keySet()){
               System.out.println("Get successful on key: " + k);
           }
        });
    }

    public void handlePut(Map<Long, byte[]> values) throws IOException {
        CompletableFuture<Void> req = this.client.put(values);

        // TODO colocar aqui exceptions
        req.thenAccept(n -> {
            System.out.println("Put successful.");
        });
    }
}
