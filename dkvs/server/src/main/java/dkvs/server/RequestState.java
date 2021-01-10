package dkvs.server;

import dkvs.server.identity.ServerId;
import dkvs.shared.Message;
import dkvs.shared.Network;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RequestState {

   //TODO private final Queue<Integer> waitingResponseRequests;

    // This map represents the active connections, that make requests to the server. The id is the
    // client UUID and the value is the network connecting the server with the client.
    private final Map<String, Network> connections;

    // This map represents the currently running get requests, where the Id is the request UUID,
    // and the value is the map that is being built based on the answer received from the other servers
    private Map<String, Map<Long, byte[]>> getRequests;

    // This map represents the currently running put requests, where the Id is the request UUID,
    // and the value is a map containing the servers that the put execute was requested containing
    // a boolean that represents if we already received a success message from that server or no.
    private Map<String, Map<ServerId, Boolean>> putRequests;

    public RequestState() {
        this.connections = new ConcurrentHashMap<>();
        this.getRequests = new ConcurrentHashMap<>();
        this.putRequests = new ConcurrentHashMap<>();
    }

    /**
     * Method that verifies if the connection is already inserted in the connections map.
     * @param clientUUID The client UUID.
     * @param network    The network connecting the server and the client.
     */
    public void addConnection(final String clientUUID, final Network network){
        if (!connections.containsKey(clientUUID)) {
            this.connections.put(clientUUID, network);
        }
    }

    /**
     * Method that initializes the received put request from a client.
     * @param putMessageUUID The UUID that represents the put request.
     * @param serversToContact The set of servers needed to contact to send the response to the client.
     */
    public void newPutRequest(final String putMessageUUID, final Set<ServerId> serversToContact){
        Map<ServerId, Boolean> putReplies = new ConcurrentHashMap<>();
        serversToContact.stream().map(server -> putReplies.put(server, false));

        this.putRequests.put(putMessageUUID, putReplies);
    }

    /**
     * Method that once a receive of a put reply, updates the put request to confirm that
     * already received the response for the put execute message.
     * @param putMessageUUID The UUID that represents the put request.
     * @param serverId       The server id of the server that was contacted.
     */
    public void putReply(final String putMessageUUID, final ServerId serverId){
        if (this.putRequests.containsKey(putMessageUUID)){
            if (this.putRequests.get(putMessageUUID).containsKey(serverId)){
                this.putRequests.get(putMessageUUID).replace(serverId, true);
            }
        }
    }

    /**
     * Method that given a put request UUID, verifies if the request is complete.
     * @param putMessageUUID The request UUID.
     * @return True if the request is complete.
     */
    public boolean isPutRequestComplete(final String putMessageUUID){
        return this.putRequests.containsKey(putMessageUUID) && !this.putRequests.get(putMessageUUID).containsValue(false);
    }

    /**
     * Method that removes a put request given a put request UUID.
     * @param putMessageUUID The request UUID.
     */
    public void removePutRequest(final String putMessageUUID){
        this.putRequests.remove(putMessageUUID);
    }

    public void newGetRequest(final String getMessageUUID){
        getRequests.put(getMessageUUID, new HashMap<>());
    }

    /**
     * Method that given an clientUUID, verifies if that clients has an existing connection with the server
     * and sends the request response to the client.
     * @param clientUUID The client UUID (could be a "real" client or other server)
     * @param message The message to send to the client.
     * @throws IOException
     */
    public void sendRequestResponse(final String clientUUID, final Message message) throws IOException {
        if (connections.containsKey(clientUUID)){
            Network network = connections.get(clientUUID);

            network.send(message).thenAccept(v -> {
                System.out.println("> Message sent to client " + clientUUID + " with success.");
            });
        }
    }
}
