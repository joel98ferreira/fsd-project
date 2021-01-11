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

    // This map represents the currently running requests, where the Id is the request UUID,
    // and the value is a map containing the servers that that request need to contact in order
    // to return a response for the client as value we have a boolean that represents if we already
    // received a response message from that server or no.
    private Map<String, Map<ServerId, Boolean>> serverRequests;

    public RequestState() {
        this.connections = new ConcurrentHashMap<>();
        this.getRequests = new ConcurrentHashMap<>();
        this.serverRequests = new ConcurrentHashMap<>();
    }

    /**
     * Method that verifies if the connection is already inserted in the connections map, and if
     * it's not then it inserts the connection.
     * @param clientUUID The client UUID.
     * @param network    The network connecting the server and the client.
     */
    public void addConnection(final String clientUUID, final Network network){
        if (!connections.containsKey(clientUUID)) {
            this.connections.put(clientUUID, network);
        }
    }

    /**
     * Method that initializes the received request from a client.
     * @param messageUUID The UUID that represents the request.
     * @param serversToContact The set of servers needed to contact to send the response to the client.
     */
    public void newRequest(final String messageUUID, final Set<ServerId> serversToContact){
        Map<ServerId, Boolean> requestReplies = new ConcurrentHashMap<>();
        serversToContact.stream().map(server -> requestReplies.put(server, false));

        this.serverRequests.put(messageUUID, requestReplies);
    }

    /**
     * Method that once the receive of a message reply, updates the server request to confirm that
     * already received the response for the execute message.
     * @param messageUUID The UUID that represents the request.
     * @param serverId    The server id of the server that was contacted.
     */
    public void newReply(final String messageUUID, final ServerId serverId){
        if (this.serverRequests.containsKey(messageUUID)){
            if (this.serverRequests.get(messageUUID).containsKey(serverId)){
                this.serverRequests.get(messageUUID).replace(serverId, true);
            }
        }
    }

    /**
     * Method that given a request UUID, verifies if the request is complete.
     * @param messageUUID The request UUID.
     * @return True if the request is complete.
     */
    public boolean isRequestComplete(final String messageUUID){
        return this.serverRequests.containsKey(messageUUID) && !this.serverRequests.get(messageUUID).containsValue(false);
    }

    /**
     * Method that removes a request given a request UUID.
     * @param messageUUID The request UUID.
     */
    public void removeRequest(final String messageUUID){
        this.serverRequests.remove(messageUUID);
    }

    /**
     * Method that updates a currently running GET Request, by building the result map.
     * @param messageUUID The GET request message uuid.
     * @param values The values of the map to insert in the map that we are building.
     */
    public void updateGetRequest(final String messageUUID, Map<Long, byte[]> values){
        if (this.serverRequests.containsKey(messageUUID)){
            if (!this.getRequests.containsKey(messageUUID)){
                this.getRequests.put(messageUUID, new HashMap<>());
            }
            this.getRequests.get(messageUUID).putAll(values);
        }
    }

    /**
     * Method that by a given message UUID return's the get request value (the map with the requested keys).
     * @param messageUUID The message UUID.
     * @return The map with the requested keys.
     */
    public Map<Long, byte[]> getGetRequestValue(String messageUUID){
        if (this.getRequests.containsKey(messageUUID)){
            return this.getRequests.get(messageUUID);
        }
        return new HashMap<>();
    }

    /**
     * Method that by a given message UUID, removes the get request from the currently running get requests
     * and it removes the request from the server requests.
     * @param messageUUID The message UUID.
     */
    public void removeGetRequest(String messageUUID){
        this.serverRequests.remove(messageUUID);
        this.getRequests.remove(messageUUID);
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
                System.out.println("> Message with type " + message.getType().toString() + " sent to client " + clientUUID + " with success.");
            });
        }
    }
}
