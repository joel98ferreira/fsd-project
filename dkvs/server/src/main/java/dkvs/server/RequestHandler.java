package dkvs.server;

import dkvs.server.identity.ServerId;
import dkvs.server.network.ServerNetwork;
import dkvs.server.network.ServerResponseContent;
import dkvs.shared.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    // Scheduler used to validate the request's received from others servers before timeout
    private static final ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();

    private static final boolean DEBUG = true;

    public RequestHandler(ServerConfig serverConfig, ServerNetwork serverNetwork, KeyValueStore keyValueStore) {
        this.localServerId = Objects.requireNonNull(serverConfig.getLocalServerId());
        this.serverNetwork = Objects.requireNonNull(serverNetwork);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.requestState = new RequestState();
        this.consistencyHash = new ConsistencyHash(Objects.requireNonNull(serverConfig));
    }

    /**
     * Method that handles a received message and execute the correct method to that request.
     * It receives the message, the client UUID and the network connecting the server and the client.
     * @param message The received message.
     * @param clientUUID The client UUID.
     * @param network The network connecting the server and the client.
     */
    public void handleMessage(final Message message, final String clientUUID, final Network network) {

        // Add the connection to the request state, if the connection is already in the request state then it's ignored
        requestState.addConnection(clientUUID, network);

        if (DEBUG) System.out.println("> Received a message from: " + clientUUID);

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
                handleGetRequest(message, clientUUID);
                break;
            case GET_EXECUTE: // Execution received from a known peer (server)
                if (DEBUG) System.out.println("> Received a GET EXECUTE!");
                handleGetExecute(message, clientUUID);
                break;
            case PUT_REPLY:  // Receive a put ACK from the remote server where it has been executed
                if (DEBUG) System.out.println("> Received a PUT REPLY!");
                handlePutReply(message, clientUUID);
                break;
            case GET_REPLY:  // Receive the map containing the requested keys
                if (DEBUG) System.out.println("> Received a GET REPLY!");
                handleGetReply(message, clientUUID);
                break;
            default:         // Unknown message type
                if (DEBUG) System.out.println("> Unknown Request Type!");
                // TODO: Throw exception
                break;
        }
    }


    /**********************************************************************************************************
     *                                              PUT REQUESTS                                              *
     **********************************************************************************************************/

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
            requestState.newRequest(message.getId(), mappedByServerPut.keySet());

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
                    this.serverNetwork.sendAndReceive(serverPut.getKey(), putExecute, this);
                }
            }

            // TODO: ESPERAR PELA RESPOSTA 8SEGUNDOS E CASO NAO RECEBER MANDAR ERRO
            // es.schedule(() -> {

              // verificaPedidoGet(pg.id);

            //}, Config.TIMEOUT, TimeUnit.SECONDS);


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

            if (DEBUG) System.out.println("> PUT EXECUTE completed successfully.");

            // Send a message acknowledging the put execution successfully
            Message response = new Message(message.getId(), RequestType.PUT_REPLY, new ServerResponseContent(this.localServerId, 200,  null)); // 200 - OK
            requestState.sendRequestResponse(clientUUID, response);

        } catch (Exception e){
            System.err.println("Error in PUT EXECUTE");
            e.printStackTrace();
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that handles the ACK from the server where the PUT Execute message was sent to.
     * @param message The ACK message for the PUT Execute.
     * @param clientUUID The client UUID.
     */
    private void handlePutReply(final Message message, final String clientUUID){
        try {
            ServerResponseContent putInfo = (ServerResponseContent) message.getContent();

            if (DEBUG) System.out.println("> PUT REPLY from: " + putInfo.getServerId().toString());

            if (putInfo.getStatusCode() == 200){
                // Handle the put reply message and update the request state
                handlePutRequestUpdate(clientUUID, message.getId(), putInfo.getServerId());
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
        requestState.newReply(messageUUID, serverId);

        // Verify if the put request is completed
        if (requestState.isRequestComplete(messageUUID)){
            if (DEBUG) System.out.println("> PUT REQUEST is complete, sending result to client.");

            // Send a success message to the client
            Message response = new Message(messageUUID, RequestType.PUT_REPLY, 200);
            requestState.sendRequestResponse(clientUUID, response);

            // Remove the request
            requestState.removeRequest(messageUUID);
        } else {
            if (DEBUG) System.out.println("> PUT REQUEST is not completed, waiting for response from the contacted servers.");
        }
    }


    /**********************************************************************************************************
     *                                              GET REQUESTS                                              *
     **********************************************************************************************************/

    /**
     * Handle the request received on this current server and contact the servers needed to
     * provide an answer to the client.
     * @param message The request message received from the client.
     * @param clientUUID The client UUID.
     */
    private void handleGetRequest(final Message message, final String clientUUID){
        try {
            Collection<Long> keys = (Collection<Long>) message.getContent();

            // Map the get request by server
            Map<ServerId, Collection<Long>> mappedByServerGet = consistencyHash.mapServerGetRequest(keys);

            // Insert the get request in the currently running requests
            requestState.newRequest(message.getId(), mappedByServerGet.keySet());

            for (Map.Entry<ServerId, Collection<Long>> serverGet : mappedByServerGet.entrySet()){

                // Verify if any get request is to the current server
                if (serverGet.getKey().equals(this.localServerId)){
                    Collection<Long> myGetReq = mappedByServerGet.get(this.localServerId);

                    // Obtain the values from the key value store
                    Map<Long, byte[]> values = this.keyValueStore.get(myGetReq);

                    // Update the status of the get reply message, since this get request was to the current server
                    handleGetRequestUpdate(clientUUID, message.getId(), serverGet.getKey(), values);

                } else { // Send the request to the corresponding server TODO: concorrentemente?

                    if (DEBUG) System.out.println("> Sending GET EXECUTE to: " + serverGet.getKey().toString());

                    // Create a new message with GET_EXECUTE type TODO: Mandar relogios logicos
                    Message getExecute = new Message(message.getId(), RequestType.GET_EXECUTE, serverGet.getValue());

                    // Send the message to the server
                    this.serverNetwork.sendAndReceive(serverGet.getKey(), getExecute, this);
                }
            }
        } catch (Exception e){
            System.err.println("Error in GET REQUEST");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Execution request by another server, to provide the value for the requested
     * keys in the key value store.
     * @param message The message from the other server.
     * @param clientUUID The client UUID.
     */
    private void handleGetExecute(final Message message, final String clientUUID){
        try {
            Collection<Long> keys = (Collection<Long>) message.getContent();

            // Obtain the values from the key value store
            Map<Long, byte[]> values = this.keyValueStore.get(keys);

            if (DEBUG) System.out.println("> GET EXECUTE completed successfully.");

            // Send a message acknowledging the get execution successfully and with the obtained map
            Message response = new Message(message.getId(), RequestType.GET_REPLY, new ServerResponseContent(this.localServerId, 200, values));
            requestState.sendRequestResponse(clientUUID, response);

        } catch (Exception e){
            System.err.println("Error in GET EXECUTE");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that handles the ACK from the server where the GET Execute message was sent to.
     * @param message The ACK message for the GET Execute.
     * @param clientUUID The client UUID.
     */
    private void handleGetReply(final Message message, final String clientUUID){
        try {
            ServerResponseContent getInfo = (ServerResponseContent) message.getContent();

            if (DEBUG) System.out.println("> GET REPLY from: " + getInfo.getServerId().toString());

            // Handle the get reply message and update the request state by passing the obtained map
            handleGetRequestUpdate(clientUUID, message.getId(), getInfo.getServerId(), (Map<Long, byte[]>) getInfo.getContent());

        } catch (Exception e){
            System.err.println("Error in GET REPLY");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that is used when receiving a get reply (ACK), it updates the get req status in the the request
     * state and verifies if the request is completed. If it's completed then sends a confirmation
     * message to the client and removes the request from the currently running requests.
     * @param clientUUID The client UUID.
     * @param messageUUID The get reply message UUID.
     * @param serverId The server id of the server that replied.
     * @param values The map containing the value for the request keys, or partial values in case some key didn't exist.
     * @throws IOException
     */
    private void handleGetRequestUpdate(final String clientUUID, final String messageUUID, final ServerId serverId,
                                        final Map<Long, byte[]> values) throws IOException {

        // Update the map with info saying that the request for this server was satisfied
        requestState.newReply(messageUUID, serverId);

        // Update the GET Map
        requestState.updateGetRequest(messageUUID, values);

        // Verify if the get request is completed
        if (requestState.isRequestComplete(messageUUID)){
            if (DEBUG) System.out.println("> GET REQUEST is complete, sending result to client.");

            // Obtain the result map
            Map<Long, byte[]> resultMap = requestState.getGetRequestValue(messageUUID);

            // Send a success message to the client
            Message response = new Message(messageUUID, RequestType.GET_REPLY, resultMap);
            requestState.sendRequestResponse(clientUUID, response);

            // Remove the request, it removes it from the currently running requests and from the running get requests.
            requestState.removeGetRequest(messageUUID);
        } else {
            if (DEBUG) System.out.println("> GET REQUEST is not completed, waiting for response from the contacted servers.");
        }
    }
}
