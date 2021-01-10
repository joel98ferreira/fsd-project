package dkvs.server;

import dkvs.server.identity.ServerId;
import dkvs.server.network.ServerNetwork;
import dkvs.shared.Message;
import dkvs.shared.Network;
import dkvs.shared.RequestType;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.*;

public class RequestHandler {

    // Local Server ID
    private final ServerId localServerId;

    private final ServerNetwork serverNetwork;

    // Key Value Store
    private final KeyValueStore keyValueStore;

    // Represents the current state of the existing requests.
    private final RequestState requestState;

    // Consistency Hash Algorithm used to distribute the keys in the server network
    private final ConsistencyHash consistencyHash;

    private static final boolean DEBUG = true;

    public RequestHandler(ServerConfig serverConfig, ServerNetwork serverNetwork, KeyValueStore keyValueStore) {
        this.localServerId = Objects.requireNonNull(serverConfig.getLocalServerId());
        this.serverNetwork = Objects.requireNonNull(serverNetwork);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.requestState = new RequestState();
        this.consistencyHash = new ConsistencyHash(Objects.requireNonNull(serverConfig));
    }

    public void handleMessage(final Message message, final String clientUUID, final Network network){

        // Add the connection to the request state, if the connection is already in the request state then it's ignored
        requestState.addConnection(clientUUID, network);

        switch (message.getType()){
            case PUT_REQUEST: // Request received from a client
                if (DEBUG) System.out.println("> Received a PUT REQUEST!");
                handlePutRequest(message, clientUUID);
                break;
            case PUT_EXECUTE: // Execution received from a known peer (server)
                if (DEBUG) System.out.println("> Received a PUT EXECUTE!");
                handlePutExecute(message, clientUUID);
                break;
            case GET_REQUEST: // Request received from a client
                if (DEBUG) System.out.println("> Received a GET REQUEST!");
                // handleGetRequest(message, clientUUID);
                break;
            case GET_EXECUTE: // Execution received from a known peer (server)
                if (DEBUG) System.out.println("> Received a GET EXECUTE!");
               // handleGetExecute(message, clientUUID);
                break;
            case PUT_REPLY:  // Receive a put ACK from the remote server where it has been executed
                if (DEBUG) System.out.println("> Received a PUT REPLY!");
                handlePutReply(message, clientUUID);
                break;
            case GET_REPLY:  // Receive the map containing the requested keys
                if (DEBUG) System.out.println("> Received a GET REPLY!");
                // handleGetReply(message, clientUUID, network);
                break;
            default:
                // Unknown message
                // TODO: Throw exception
                break;
        }
    }

    /**
     * Handle the request received on this current server and contact the servers needed to
     * provide an answer to the client.
     * @param message The request message received from the client.
     * @param clientUUID The client UUID.
     */
    private void handlePutRequest(final Message message, final String clientUUID){
        try {
            Map<Long, byte[]> values = (Map<Long, byte[]>) message.getContent();

            // Map the put by server
            Map<ServerId, Map<Long, byte[]>> mappedByServerPut = consistencyHash.mapServerPutRequest(values);

            // Insert the put request in the currently running requests
            requestState.newPutRequest(message.getId(), mappedByServerPut.keySet());

            for (Map.Entry<ServerId, Map<Long, byte[]>> serverPut : mappedByServerPut.entrySet()){

                // Verify if any put request is to the current server
                if (serverPut.getKey().equals(this.localServerId)){
                    Map<Long, byte[]> myPutReq = mappedByServerPut.get(this.localServerId);

                    // Insert the values in my key value store
                    this.keyValueStore.put(myPutReq);

                    // Update the status of the put reply message, since this put request was to the current server
                    handlePutRequestUpdate(clientUUID, message.getId(), serverPut.getKey());

                } else { // Send the request to the corresponding server TODO: concorrentemente?

                    if (DEBUG) System.out.println("> Sending PUT EXECUTE to: " + serverPut.getKey().toString());

                    // Create a new message with PUT_EXECUTE type TODO: Mandar relogios logicos
                    Message putExecute = new Message(message.getId(), RequestType.PUT_EXECUTE, serverPut.getValue());

                    // Send the message to the server
                    this.serverNetwork.send(serverPut.getKey(), putExecute);
                }
            }
        } catch (Exception e){
            System.err.println("Error in PUT REQUEST");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Execution request by a another server, to insert the determined keys in the key
     * value store.
     * @param message The message from the other server.
     * @param clientUUID The client UUID.
     */
    private void handlePutExecute(final Message message, final String clientUUID){
        try {
            Map<Long, byte[]> values = (Map<Long, byte[]>) message.getContent();

            // Insert the received values in the key value store
            this.keyValueStore.put(values);

            // Send a message acknowledging the put execution successfully
            Message response = new Message(message.getId(), RequestType.PUT_REPLY, new Pair<>(this.localServerId, 200)); // 200 - OK
            requestState.sendRequestResponse(clientUUID, response);

        } catch (Exception e){
            System.err.println("Error in PUT EXECUTE");
            // TODO: Thrown unknown request
        }
    }

    /**
     *
     * @param message
     */
    private void handlePutReply(final Message message, final String clientUUID){
        try {
            Pair<ServerId, Integer> putInfo = (Pair<ServerId, Integer>) message.getContent();

            if (putInfo.getValue1() == 200){

                // Handle the put reply message and update the request state
                handlePutRequestUpdate(clientUUID, message.getId(), putInfo.getValue0());
            }else{
                // TODO: Throw error
            }

        } catch (Exception e){
            System.err.println("Error in PUT REPLY");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that is used when receiving a put reply, it updates the put reply in the the request
     * state and verifies if the request is completed. If it's completed then sends a confirmation
     * message to the client and removes the request from the currently running requests.
     * @param clientUUID The client UUID.
     * @param messageUUID The put reply message UUID.
     * @param serverId The server id of the server that replied.
     * @throws IOException
     */
    private void handlePutRequestUpdate(final String clientUUID, final String messageUUID, final ServerId serverId) throws IOException {
        // Update the map with info saying that the request for this server was satisfied
        requestState.putReply(messageUUID, serverId);

        // Verify if the put request is completed
        if (requestState.isPutRequestComplete(messageUUID)){
            if (DEBUG) System.out.println("> PUT REQUEST is complete, sending result to client.");

            // Send a success message to the client
            Message response = new Message(messageUUID, RequestType.PUT_REPLY, 200);
            requestState.sendRequestResponse(clientUUID, response);

            // Remove the request
            requestState.removePutRequest(messageUUID);
        }
    }

    /**
     * Handle the request received on this current server and contact the servers needed to
     * provide an answer to the client.
     * @param message The request message received from the client.
     * @param clientUUID The client UUID.
     */
    /*
    private void handleGetRequest(final Message message, final String clientUUID){
        Collection<Long> values = (Collection<Long>) message.getContent();

        // Generate a UUID for this request
        String uuid = UUID.randomUUID().toString();

        // Insert the request in the currently running get requests map
        getRequests.put(uuid, null);

        // Map the get by server
        Map<ServerId, Collection<Long>> mappedByServerGet = consistencyHash.mapServerGetRequest(values);

        for (Map.Entry<ServerId, Collection<Long>> serverGet : mappedByServerGet.entrySet()){

            // Verify if any get request is to the current server
            if (serverGet.getKey().equals(server.getServerId())){
                Collection<Long> myGetReq = mappedByServerGet.get(server.getServerId());

                // Get the corresponding keys
                Map<Long, byte[]> map = server.getKeyValueStore().get(myGetReq);

                if (getRequests.get(uuid) == null) {
                    getRequests.replace(uuid, map);
                } else {
                    getRequests.get(uuid).putAll(map);
                }

                // TODO: store this in client queue


            } else { // Send the request to each server TODO: concorrentemente?

                // Create a new message with GET_EXECUTE type TODO: Mandar relogios logicos
                Message getExecute = new Message(RequestType.GET_EXECUTE, serverGet.getValue());

                // Send the message to the the server
                server.getServerNetwork().send(serverGet.getKey(), getExecute);
            }
        }
    }
*/
    /**
     * Execution request by a another server, to provide the determined keys.
     * @param message The message from the other server.
     */
    private void handleGetExecute(Message message){


    }
}
