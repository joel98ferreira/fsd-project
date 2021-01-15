package dkvs.server;

import dkvs.server.identity.ClientId;
import dkvs.server.identity.ServerId;
import dkvs.server.network.ScalarLogicalClock;
import dkvs.server.network.ServerNetwork;
import dkvs.server.network.ServerRequestMessageContent;
import dkvs.server.network.ServerResponseMessageContent;
import dkvs.shared.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class RequestHandler {

    // Local Server ID
    private final ServerId localServerId;

    // The Server Network
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
        this.requestState = new RequestState(serverConfig);
        this.consistencyHash = new ConsistencyHash(Objects.requireNonNull(serverConfig));
    }

    /**
     * Method that handles a received message and execute the correct method to that request.
     * It receives the message, the client UUID and the network connecting the server and the client.
     * @param message The received message.
     * @param clientId The client UUID.
     * @param network The network connecting the server and the client.
     */
    public void handleMessage(final Message message, final ClientId clientId, final Network network) {

        if (!clientId.getClientId().contains("Server")) {
            // Add the connection to the request state, if the connection is already in the request state then it's ignored
            requestState.addConnection(clientId, network);
            if (DEBUG) System.out.println("> Received a message from client: " + clientId + " with id: " + message.getId());
        }else {
            // Request from the server network add the clock and calculate the max clock for this server
            requestState.newClock(clientId, message.getContent());
            if (DEBUG) System.out.println("> Received the response to a request in the Server Network from: " + clientId.getClientId() + " with id: " + message.getId());
        }

        // TODO: VALIDATE IF CAN BE SATISFIED

        switch (message.getType()){
            case PUT_REQUEST: // Request received from a client
                if (DEBUG) System.out.println("> Received a PUT REQUEST!");
                handlePutRequest(message, clientId);
                break;
            case PUT_EXECUTE: // Execution received from a known peer (server)
                if (DEBUG) System.out.println("> Received a PUT EXECUTE!");
                handlePutExecute(message, clientId);
                break;
            case GET_REQUEST: // Request received from a client
                if (DEBUG) System.out.println("> Received a GET REQUEST!");
                handleGetRequest(message, clientId);
                break;
            case GET_EXECUTE: // Execution received from a known peer (server)
                if (DEBUG) System.out.println("> Received a GET EXECUTE!");
                handleGetExecute(message, clientId);
                break;
            case PUT_REPLY:  // Receive a put ACK from the remote server where it has been executed
                if (DEBUG) System.out.println("> Received a PUT REPLY!");
                handlePutReply(message);
                break;
            case GET_REPLY:  // Receive the map containing the requested keys
                if (DEBUG) System.out.println("> Received a GET REPLY!");
                handleGetReply(message);
                break;
            case PUT_LOCK:   // Receive a message from a process to obtain lock in some keys and to update the clocks
                if (DEBUG) System.out.println("> Received a PUT LOCK!");
                break;
            case PUT_UNLOCK: // Receive a message from a process to release lock in some keys and to update the clocks
                if (DEBUG) System.out.println("> Received a PUT UNLOCK!");
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
     * @param message  The request message received from the client.
     * @param clientId The client UUID.
     */
    private void handlePutRequest(final Message message, final ClientId clientId){
        try {
            Map<Long, byte[]> values = (Map<Long, byte[]>) message.getContent();

            // Map the put by server
            Map<ServerId, Map<Long, byte[]>> mappedByServerPut = consistencyHash.mapServerPutRequest(values);

            // Insert the put request in the currently running requests
            requestState.newRequest(clientId, message.getId(), mappedByServerPut.keySet());

            // TODO: Insert the blocked keys

            for (Map.Entry<ServerId, Map<Long, byte[]>> serverPut : mappedByServerPut.entrySet()){

                // TODO: Validate if I can run the PUT REQ

                // Verify if any put request is to the current server
                if (serverPut.getKey().equals(this.localServerId)){
                    Map<Long, byte[]> myPutReq = mappedByServerPut.get(this.localServerId);

                    // Insert the values in my key value store
                    this.keyValueStore.put(myPutReq);

                    // Update the status of the put reply message, since this put request was to the current server
                    handlePutRequestUpdate(message.getId(), serverPut.getKey());

                } else { // Send the request to the corresponding server

                    // Determine the Key set that are not directly to the PUT EXECUTE server, so it can lock on other keys
                    // to known when it receives the PUT UNLOCK that it can finish the transaction and make the previous version
                    // values deleted.
                    Collection<Long> otherPutKeys = new ArrayList<>(values.keySet());
                    otherPutKeys.removeAll(serverPut.getValue().entrySet());

                    // Create a new message with PUT_EXECUTE type, the PUT_EXECUTE message has an implicit LOCK on that key set
                    // This message is sent with my current logical clock, the values that I want to insert and lock in that server
                    // and the other values that are relative to the other requests that I made to other servers, so it can "lock"
                    // that keys, and just assumes that all of the values should be written to the definitive map when it receives
                    // all unlocks from all of the keys of that transaction.
                    Message putExecute = new Message(message.getId(), RequestType.PUT_EXECUTE,
                            new ServerRequestMessageContent(
                                    requestState.getMyLogicalClock(),
                                    new AbstractMap.SimpleEntry<>(serverPut.getValue(), otherPutKeys)
                            )
                    );

                    if (DEBUG) System.out.println("> Sending PUT EXECUTE to: " + serverPut.getKey().toString());

                    // Send the message with PUT_EXECUTE to the server
                    this.serverNetwork.send(serverPut.getKey(), putExecute, this);

                    // Update my logical clock with a send event
                    this.requestState.updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);
                }
            }

            // Send the PUT LOCK to other peers that are currently not in the PUT EXECUTE servers set
            List<ServerId> remoteServersIdsToSendLock = this.serverNetwork.getRemoteServersIds();
            remoteServersIdsToSendLock.removeAll(mappedByServerPut.keySet());

            if (remoteServersIdsToSendLock.size() != 0){
                for (ServerId serverId : remoteServersIdsToSendLock){

                    // Collection with all keys that are currently running in the transaction
                    Collection<Long> putKeys = new ArrayList<>(values.keySet());

                    // Create a new message with PUT_LOCK type, the PUT_LOCK is just send for all other
                    // servers that are not included in the transaction. This will be useful since after other
                    // server receives a PUT_LOCK it can send a CLOCK_UPDATE message
                    Message putLock = new Message(message.getId(), RequestType.PUT_LOCK,
                            new ServerRequestMessageContent(
                                    requestState.getMyLogicalClock(),
                                    putKeys
                            )
                    );

                    if (DEBUG) System.out.println("> Sending PUT LOCK to: " + serverId.toString());

                    // Send the message with PUT_LOCK to the server
                    this.serverNetwork.send(serverId, putLock, this);

                    // Update my logical clock with send event
                    this.requestState.updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);
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
     * @param message  The message from the other server.
     * @param clientId The client UUID.
     */
    private void handlePutExecute(final Message message, final ClientId clientId){
        try {
            ServerRequestMessageContent requestMessageContent = (ServerRequestMessageContent) message.getContent();

            if (DEBUG) System.out.println("> The received clock from: " + clientId  + " is: " + requestMessageContent.getLogicalClock().toString());

            AbstractMap.SimpleEntry<Map<Long, byte[]>, Collection<Long>> putExecuteInfo = (AbstractMap.SimpleEntry<Map<Long, byte[]>, Collection<Long>>) requestMessageContent.getContent();

            // Validate if the received message can be considered to insert in the queue
            boolean canExecuteRequest =  requestState.isClockValidToInsertInQueue(requestMessageContent.getLogicalClock());

            if (canExecuteRequest){

                // Obtain all keys of the transaction
                Collection<Long> lockedKeys = putExecuteInfo.getValue();
                lockedKeys.addAll(putExecuteInfo.getKey().keySet());

                // Insert in the locking queue
                this.requestState.insertInLockingQueue(message.getId(), lockedKeys);

                // TODO: am i the first?
                // Update the values in the Key Value Store and save the previous ones
                this.keyValueStore.putPrepare(putExecuteInfo.getKey());

                if (DEBUG) System.out.println("> PUT Prepare is done, waiting for unlocks to complete PUT.");

                // Send a message acknowledging the put execution successfully
                Message response = new Message(message.getId(), RequestType.PUT_REPLY, new ServerResponseMessageContent(this.localServerId, 200,  null)); // 200 - OK
                requestState.sendRequestResponse(clientId, response);


            } else {
                // TODO: INSERIR NA QUEUE DE PENDENTES
            }
        } catch (Exception e){
            System.err.println("Error in PUT EXECUTE");
            e.printStackTrace();
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that handles the ACK from the server where the PUT Execute message was sent to.
     * @param message The ACK message for the PUT Execute.
     */
    private void handlePutReply(final Message message){
        try {
            ServerResponseMessageContent putInfo = (ServerResponseMessageContent) message.getContent();

            if (DEBUG) System.out.println("> PUT REPLY from: " + putInfo.getServerId().toString());

            if (putInfo.getStatusCode() == 200){
                // Handle the put reply message and update the request state
                handlePutRequestUpdate(message.getId(), putInfo.getServerId());
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
     * @param messageId The put reply message UUID.
     * @param serverId The server id of the server that replied.
     * @throws IOException
     */
    private void handlePutRequestUpdate(final MessageId messageId, final ServerId serverId) throws IOException {
        // Update the map with info saying that the request for this server was satisfied
        requestState.newReply(messageId, serverId);

        // Verify if the put request is completed
        if (requestState.isRequestComplete(messageId)){
            if (DEBUG) System.out.println("> PUT REQUEST is complete, sending result to client.");

            // Send a success message to the client
            Message response = new Message(messageId, RequestType.PUT_REPLY, 200);
            requestState.sendOriginalRequestResponse(messageId, response);

            // Remove the request
            requestState.removeRequest(messageId);
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
     * @param message  The request message received from the client.
     * @param clientId The client UUID.
     */
    private void handleGetRequest(final Message message, final ClientId clientId){
        try {
            Collection<Long> keys = (Collection<Long>) message.getContent();

            // Map the get request by server
            Map<ServerId, Collection<Long>> mappedByServerGet = consistencyHash.mapServerGetRequest(keys);

            // Insert the get request in the currently running requests
            requestState.newRequest(clientId, message.getId(), mappedByServerGet.keySet());

            for (Map.Entry<ServerId, Collection<Long>> serverGet : mappedByServerGet.entrySet()){

                // Verify if any get request is to the current server
                if (serverGet.getKey().equals(this.localServerId)){
                    Collection<Long> myGetReq = mappedByServerGet.get(this.localServerId);

                    // Obtain the values from the key value store
                    Map<Long, byte[]> values = this.keyValueStore.get(myGetReq);

                    // Update the status of the get reply message, since this get request was to the current server
                    handleGetRequestUpdate(message.getId(), serverGet.getKey(), values);

                } else { // Send the request to the corresponding server TODO: concorrentemente?

                    if (DEBUG) System.out.println("> Sending GET EXECUTE to: " + serverGet.getKey().toString());

                    // Create a new message with GET_EXECUTE type TODO: Mandar relogios logicos
                    Message getExecute = new Message(message.getId(), RequestType.GET_EXECUTE, serverGet.getValue());

                    // Send the message to the server
                    this.serverNetwork.send(serverGet.getKey(), getExecute, this);
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
     * @param message  The message from the other server.
     * @param clientId The client UUID.
     */
    private void handleGetExecute(final Message message, final ClientId clientId){
        try {
            Collection<Long> keys = (Collection<Long>) message.getContent();

            // Obtain the values from the key value store
            Map<Long, byte[]> values = this.keyValueStore.get(keys);

            if (DEBUG) System.out.println("> GET EXECUTE completed successfully.");

            // Send a message acknowledging the get execution successfully and with the obtained map
            Message response = new Message(message.getId(), RequestType.GET_REPLY, new ServerResponseMessageContent(this.localServerId, 200, values));
            requestState.sendRequestResponse(clientId, response);

        } catch (Exception e){
            System.err.println("Error in GET EXECUTE");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that handles the ACK from the server where the GET Execute message was sent to.
     * @param message The ACK message for the GET Execute.
     */
    private void handleGetReply(final Message message){
        try {
            ServerResponseMessageContent getInfo = (ServerResponseMessageContent) message.getContent();

            if (DEBUG) System.out.println("> GET REPLY from: " + getInfo.getServerId().toString());

            // Handle the get reply message and update the request state by passing the obtained map
            handleGetRequestUpdate(message.getId(), getInfo.getServerId(), (Map<Long, byte[]>) getInfo.getContent());

        } catch (Exception e){
            System.err.println("Error in GET REPLY");
            // TODO: Thrown unknown request
        }
    }

    /**
     * Method that is used when receiving a get reply (ACK), it updates the get req status in the the request
     * state and verifies if the request is completed. If it's completed then sends a confirmation
     * message to the client and removes the request from the currently running requests.
     * @param messageId The get reply message UUID.
     * @param serverId The server id of the server that replied.
     * @param values The map containing the value for the request keys, or partial values in case some key didn't exist.
     * @throws IOException
     */
    private void handleGetRequestUpdate(final MessageId messageId, final ServerId serverId, final Map<Long, byte[]> values) throws IOException {

        // Update the map with info saying that the request for this server was satisfied
        requestState.newReply(messageId, serverId);

        // Update the GET Map
        requestState.updateGetRequest(messageId, values);

        // Verify if the get request is completed
        if (requestState.isRequestComplete(messageId)){
            if (DEBUG) System.out.println("> GET REQUEST is complete, sending result to client.");

            // Obtain the result map
            Map<Long, byte[]> resultMap = requestState.getGetRequestValue(messageId);

            // Send a success message to the client
            Message response = new Message(messageId, RequestType.GET_REPLY, resultMap);
            requestState.sendOriginalRequestResponse(messageId, response);

            // Remove the request, it removes it from the currently running requests and from the running get requests.
            requestState.removeGetRequest(messageId);
        } else {
            if (DEBUG) System.out.println("> GET REQUEST is not completed, waiting for response from the contacted servers.");
        }
    }

    /**********************************************************************************************************
     *                                          AUXILIARY METHODS                                             *
     **********************************************************************************************************/

    public void removeConnection(String clientUUID){

    }
}
