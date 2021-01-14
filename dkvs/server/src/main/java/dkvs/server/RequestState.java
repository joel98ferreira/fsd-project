package dkvs.server;

import dkvs.server.identity.ClientId;
import dkvs.server.identity.ServerId;
import dkvs.shared.Message;
import dkvs.shared.MessageId;
import dkvs.shared.Network;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RequestState {

   //TODO private final Queue<Integer> waitingResponseRequests;

    // This map represents the active connections, that make requests to the server. The id is the
    // client UUID and the value is the network connecting the server with the client.
    private final Map<ClientId, Network> connections;

    // This map represents the currently running get requests, where the Id is the request UUID,
    // and the value is the map that is being built based on the answer received from the other servers
    private final Map<MessageId, Map<Long, byte[]>> getRequests;

    // This map represents the currently running requests (PUT or GET), where the Id is the request UUID,
    // and the value is pair that contains in the left the client uuid of the original client, the one
    // that initially made the request, at the right a map containing the servers which we need to
    // contact in order to return a response for the client as value we have a boolean that represents
    // if we already received a response message from that server or no. We opted for this design,
    // since when we receive replies from contacted servers we just receive in the response the request
    // UUID and not the client uuid that originally requested it.
    private final Map<MessageId, Map.Entry<ClientId, Map<ServerId, Boolean>>> serverRequests;

    public RequestState() {
        this.connections = new ConcurrentHashMap<>();
        this.getRequests = new ConcurrentHashMap<>();
        this.serverRequests = new ConcurrentHashMap<>();
    }

    /**
     * Method that verifies if the connection is already inserted in the connections map, and if
     * it's not then it inserts the connection.
     * @param clientId The client UUID.
     * @param network    The network connecting the server and the client.
     */
    public void addConnection(final ClientId clientId, final Network network){
        if (!connections.containsKey(clientId)) {
            this.connections.put(clientId, network);
        }
    }

    /**
     * Method that initializes the received request from a client.
     * @param clientId The client UUID.
     * @param messageId The UUID that represents the request.
     * @param serversToContact The set of servers needed to contact to send the response to the client.
     */
    public void newRequest(final ClientId clientId, final MessageId messageId, final Set<ServerId> serversToContact){
        Map<ServerId, Boolean> requestReplies = new ConcurrentHashMap<>();
        serversToContact.forEach(server -> requestReplies.put(server, false));

        this.serverRequests.put(messageId, new AbstractMap.SimpleEntry<>(clientId, requestReplies));
    }

    /**
     * Method that once the receive of a message reply, updates the server request to confirm that
     * already received the response for the execute message.
     * @param messageId   The UUID that represents the request.
     * @param serverId    The server id of the server that was contacted.
     */
    public void newReply(final MessageId messageId, final ServerId serverId){
        if (this.serverRequests.containsKey(messageId)){
            if (this.serverRequests.get(messageId).getValue().containsKey(serverId)){
                this.serverRequests.get(messageId).getValue().replace(serverId, true);
            }
        }
    }

    /**
     * Method that given a request UUID, verifies if the request is complete.
     * @param messageId The request UUID.
     * @return True if the request is complete.
     */
    public boolean isRequestComplete(final MessageId messageId){
        return this.serverRequests.containsKey(messageId) && !this.serverRequests.get(messageId).getValue().containsValue(false);
    }

    /**
     * Method that removes a request given a request UUID.
     * @param messageId The request UUID.
     */
    public void removeRequest(final MessageId messageId){
        this.serverRequests.remove(messageId);
    }

    /**
     * Method that updates a currently running GET Request, by building the result map.
     * @param messageId The GET request message uuid.
     * @param values The values of the map to insert in the map that we are building.
     */
    public void updateGetRequest(final MessageId messageId, Map<Long, byte[]> values){
        if (this.serverRequests.containsKey(messageId)){
            if (!this.getRequests.containsKey(messageId)){
                this.getRequests.put(messageId, new HashMap<>());
            }
            this.getRequests.get(messageId).putAll(values);
        }
    }

    /**
     * Method that by a given message UUID return's the get request value (the map with the requested keys).
     * @param messageId The message UUID.
     * @return The map with the requested keys.
     */
    public Map<Long, byte[]> getGetRequestValue(MessageId messageId){
        if (this.getRequests.containsKey(messageId)){
            return this.getRequests.get(messageId);
        }
        return new HashMap<>();
    }

    /**
     * Method that by a given message UUID, removes the get request from the currently running get requests
     * and it removes the request from the server requests.
     * @param messageId The message UUID.
     */
    public void removeGetRequest(MessageId messageId){
        this.serverRequests.remove(messageId);
        this.getRequests.remove(messageId);
    }

    /**
     * Method that given an clientUUID (a known peer), verifies if that clients has an existing connection
     * with the server and sends the request response to the client. In this method the client is considered
     * to be a known peer and because of that it just replies to the known server, in the case of a "real"
     * client we need to first verify the request with the request id and then obtain the client id.
     * @param clientId The client UUID of the known peer.
     * @param message The message to send to the client.
     * @throws IOException
     */
    public void sendRequestResponse(final ClientId clientId, final Message message) throws IOException {
        if (connections.containsKey(clientId)){
            Network network = connections.get(clientId);

            network.send(message).thenAccept(v -> {
                System.out.println("> Message with type " + message.getType().toString() + " sent to client " + clientId + " with success.");
            });
        }
    }

    /**
     * Method that given an clientUUID, verifies if that clients has an existing connection with the server
     * and sends the request response to the client. This client is a "real" one.
     * @param messageId The request UUID.
     * @param message The message to send to the client.
     * @throws IOException
     */
    public void sendOriginalRequestResponse(final MessageId messageId, final Message message) throws IOException {
        if (this.serverRequests.containsKey(messageId)){
            ClientId originalClientId = this.serverRequests.get(messageId).getKey();
            if (connections.containsKey(originalClientId)){
                Network network = connections.get(originalClientId);

                network.send(message).thenAccept(v -> {
                    System.out.println("> Message with type " + message.getType().toString() + " sent to client " + originalClientId + " with success.");
                });
            }
        }
    }

    /**
     * Method that given a client UUID removes the connection in case it exists
     * and removes all requests from that client UUID.
     * @param clientId The client UUID which his connection is going to be deleted.
     */
    public void removeConnection(ClientId clientId){
        this.connections.remove(clientId);
    }
}
