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
        this.requestState = new RequestState(serverConfig, serverNetwork, keyValueStore);
        this.consistencyHash = new ConsistencyHash(Objects.requireNonNull(serverConfig));
    }

    /**
     * Method that handles a received message and execute the correct method to that request.
     * It receives the message, the client UUID and the network connecting the server and the client.
     * @param message The received message.
     * @param clientId The client UUID.
     * @param network The network connecting the server and the client.
     */
    public void handleMessage(final Message message, final ClientId clientId, final Network network) throws IOException {

        if (!clientId.getClientId().contains("Server")) {
            // Add the connection to the request state, if the connection is already in the request state then it's ignored
            requestState.addConnection(clientId, network);
            if (DEBUG) System.out.println("> Received a message from client: " + clientId + " with id: " + message.getId());
        }else {
            // Request from the server network add the clock and calculate the max clock for this server
            if (DEBUG) System.out.println("> Received the response to a request in the Server Network from: " + clientId.getClientId() + " with id: " + message.getId());
        }

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
            case CLOCK_UPDATE: // Clock update from other server
                if (DEBUG) System.out.println("> Received a CLOCK UPDATE!");
                handleClockUpdate(message);
                break;
            case CLOCK:       // Receive a clock message from other server
                if (DEBUG) System.out.println("> Received a CLOCK!");
                handleClock(message);
                break;
            default:         // Unknown message type
                if (DEBUG) System.out.println("> Unknown Request Type!");
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

            // Local event
            requestState.updateMyLogicalClock(ScalarLogicalClock.Event.LOCAL, null);

            for (Map.Entry<ServerId, Map<Long, byte[]>> serverPut : mappedByServerPut.entrySet()){

                // Verify if any put request is to the current server
                if (serverPut.getKey().equals(this.localServerId)){
                    Map<Long, byte[]> myPutReq = mappedByServerPut.get(this.localServerId);

                    // Determine the Key set that are not directly to the PUT EXECUTE server, so it can lock on other keys
                    // to known when it receives the PUT_REPLY that it can finish the transaction and make the previous version
                    // values deleted.
                    Collection<Long> otherPutKeys = new ArrayList<>(values.keySet());
                    otherPutKeys.removeAll(myPutReq.keySet());

                    Message m = new Message(message.getId(), RequestType.PUT_EXECUTE,
                            new ServerRequestMessageContent(
                                    localServerId,
                                    requestState.getMyLogicalClock(),
                                    new AbstractMap.SimpleEntry<>(myPutReq, otherPutKeys)
                            )
                    );

                    if (requestState.isClockValidToInsertInQueue(requestState.getMyLogicalClock())){
                        // Update the status of the put message, since this put request was to the current server
                        handlePutExecuteUpdate(m, values.keySet(), myPutReq);
                    } else {
                        if (DEBUG) System.out.println("> Clocks not valid, inserting in waiting to be process when clocks are valid.");
                        requestState.insertInWaitingToEnterQueue(message);
                    }
                } else { // Send the request to the corresponding server

                    // Determine the Key set that are not directly to the PUT EXECUTE server, so it can lock on other keys
                    // to known when it receives the PUT_REPLY that it can finish the transaction and make the previous version
                    // values deleted.
                    Collection<Long> otherPutKeys = new ArrayList<>(values.keySet());
                    otherPutKeys.removeAll(serverPut.getValue().keySet());

                    // Create a new message with PUT_EXECUTE type, the PUT_EXECUTE message has an implicit LOCK on that key set
                    // This message is sent with my current logical clock, the values that I want to insert and lock in that server
                    // and the other values that are relative to the other requests that I made to other servers, so it can "lock"
                    // that keys, and just assumes that all of the values should be written to the definitive map when it receives
                    // all unlocks from all of the keys of that transaction.
                    Message putExecute = new Message(message.getId(), RequestType.PUT_EXECUTE,
                            new ServerRequestMessageContent(
                                    localServerId,
                                    requestState.getMyLogicalClock(),
                                    new AbstractMap.SimpleEntry<>(serverPut.getValue(), otherPutKeys)
                            )
                    );

                    if (DEBUG) System.out.println("> Sending PUT EXECUTE to: " + serverPut.getKey().toString());

                    // Send the message with PUT_EXECUTE to the server
                    this.serverNetwork.send(serverPut.getKey(), putExecute);

                    // Update my logical clock with a send event
                    this.requestState.updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);
                }
            }

            // Send the CLOCK UPDATE message to other peers that are currently not in the PUT EXECUTE servers set
            List<ServerId> remoteServersIdsToSendLock = this.serverNetwork.getRemoteServersIds();
            remoteServersIdsToSendLock.removeAll(mappedByServerPut.keySet());

            if (remoteServersIdsToSendLock.size() != 0){
                sendClockUpdate(remoteServersIdsToSendLock);
            }

        } catch (Exception e){
            System.err.println("Error in PUT REQUEST");
            e.printStackTrace();
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

            // Update the received clock from the client and my clock
            requestState.newClock(clientId, requestMessageContent.getLogicalClock());

            // Process other requests waiting to enter the queue since we received a new clock
            this.requestState.processWaitingToEnterInQueue();

            // Validate if the received message can be considered to be insert in the queue
            boolean canExecuteRequest = requestState.isClockValidToInsertInQueue(requestMessageContent.getLogicalClock());

            if (canExecuteRequest){

                // Obtain all keys of the transaction
                Collection<Long> lockedKeys = putExecuteInfo.getValue();
                lockedKeys.addAll(putExecuteInfo.getKey().keySet());

                // Handle the put execute update
                handlePutExecuteUpdate(message, lockedKeys, putExecuteInfo.getKey());
            } else {
                if (DEBUG) System.out.println("> Clocks not valid, inserting in waiting to be process when clocks are valid.");
                requestState.insertInWaitingToEnterQueue(message);
            }
        } catch (Exception e){
            System.err.println("Error in PUT EXECUTE");
            e.printStackTrace();
        }
    }

    /**
     * Auxiliary method to be used in put execute.
     * @param message The received message put execute.
     * @param lockedKeys All of the locked keys.
     * @param values The map to execute the put execute.
     */
    private void handlePutExecuteUpdate(Message message, Collection<Long> lockedKeys, Map<Long, byte[]> values) throws IOException {
        // Insert this request in the priority queue
        RequestState.Status status = requestState.insertInQueue(message, lockedKeys);

        if (status == RequestState.Status.WAITING){
            if (DEBUG) System.out.println("> Found dependencies, waiting to obtain lock in some keys...");
            return;
        } else {
            if (DEBUG) System.out.println("> Obtained all locks with success! Running PUT EXECUTE!");
        }

        // Update the values in the Key Value Store and save the previous ones
        this.keyValueStore.putPrepare(values);
        if (DEBUG) System.out.println("> PUT Prepare is done.");

        // Update the received write confirmation
        RequestState.Status s = requestState.receivedWriteConfirmation(message.getId(), values.keySet());

        // Send message to all servers
        if (DEBUG) System.out.println("> Sending PUT REPLY (implicit unlock) to everyone.");
        this.requestState.sendPutReplyToAll(message.getId(), values.keySet());

        if (s == RequestState.Status.COMPLETED){
            requestState.processCompletedRequest(message.getId());
        }
    }

    /**
     * Method that handles a PUT Reply, this PUT REPLY has an implicit confirmation in the keys
     * that are stored in the running transactions, when we confirm that all of the keys in the
     * transactions already received a reply then we can simply unlock all keys and let others
     * access the new version.
     * @param message The PUT REPLY message.
     */
    private void handlePutReply(final Message message){
        try {
            ServerResponseMessageContent putInfo = (ServerResponseMessageContent) message.getContent();
            Collection<Long> confirmedKeys = (Collection<Long>) putInfo.getContent();

            // Update my logical clock
            this.requestState.newClock(new ClientId(putInfo.getServerId().toString()), putInfo.getLogicalClock());

            // Process other requests waiting to enter the queue since we received a new clock
            this.requestState.processWaitingToEnterInQueue();

            if (DEBUG) System.out.println("> PUT REPLY from: " + putInfo.getServerId().toString());

            if (putInfo.getStatusCode() == 200){
                // Update the received write confirmation
                RequestState.Status s = requestState.receivedWriteConfirmation(message.getId(), confirmedKeys);

                if (s == RequestState.Status.COMPLETED){
                    if (DEBUG) System.out.println("> PUT REQUEST is complete, sending result to client.");
                    requestState.processCompletedRequest(message.getId());
                    requestState.removeRequest(message.getId());
                } else {
                    if (DEBUG) System.out.println("> PUT REQUEST is not completed, waiting for response from the contacted servers.");
                }
            }

        } catch (Exception e){
            System.err.println("Error in PUT REPLY");
            e.printStackTrace();
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

            requestState.updateMyLogicalClock(ScalarLogicalClock.Event.LOCAL, null);

            for (Map.Entry<ServerId, Collection<Long>> serverGet : mappedByServerGet.entrySet()){

                // Verify if any get request is to the current server
                if (serverGet.getKey().equals(this.localServerId)){
                    Collection<Long> myGetReq = mappedByServerGet.get(this.localServerId);

                    // Obtain the values from the key value store
                    Map<Long, byte[]> values = this.keyValueStore.get(myGetReq);

                    // Update the status of the get reply message, since this get request was to the current server
                    handleGetRequestUpdate(message.getId(), serverGet.getKey(), values);

                    // Update my clock
                    this.requestState.updateMyLogicalClock(ScalarLogicalClock.Event.LOCAL, null);

                } else { // Send the request to the corresponding server

                    if (DEBUG) System.out.println("> Sending GET EXECUTE to: " + serverGet.getKey().toString());

                    // Create a new message with GET_EXECUTE type
                    Message getExecute = new Message(message.getId(), RequestType.GET_EXECUTE,
                            new ServerRequestMessageContent(
                                    localServerId,
                                    requestState.getMyLogicalClock(),
                                    serverGet.getValue()
                            )
                    );

                    // Send the message to the server
                    this.serverNetwork.send(serverGet.getKey(), getExecute);

                    // Update my logical clock
                    requestState.updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);
                }
            }

            // Send clock updates
            List<ServerId> clockUpdateServers = serverNetwork.getRemoteServersIds();
            clockUpdateServers.removeAll(mappedByServerGet.keySet());
            sendClockUpdate(clockUpdateServers);

        } catch (Exception e){
            System.err.println("Error in GET REQUEST");
            e.printStackTrace();
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
            ServerRequestMessageContent requestMessageContent = (ServerRequestMessageContent) message.getContent();

            Collection<Long> keys = (Collection<Long>) requestMessageContent.getContent();

            requestState.newClock(clientId, requestMessageContent.getLogicalClock());

            // Process other requests waiting to enter the queue since we received a new clock
            this.requestState.processWaitingToEnterInQueue();

            // Obtain the values from the key value store
            Map<Long, byte[]> values = this.keyValueStore.get(keys);

            if (DEBUG) System.out.println("> GET EXECUTE completed successfully.");

            // Send a message acknowledging the get execution successfully and with the obtained map and with my clock
            Message response = new Message(message.getId(), RequestType.GET_REPLY,
                    new ServerResponseMessageContent(
                            this.localServerId,
                            requestState.getMyLogicalClock(),
                            200,
                            values
                    )
            );

            requestState.sendRequestResponse(clientId, response);

            // Update my logical clock
            requestState.updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);

            // Send clock updates
            List<ServerId> clockUpdateServers = serverNetwork.getRemoteServersIds();
            clockUpdateServers.remove(requestMessageContent.getServerId());
            sendClockUpdate(clockUpdateServers);

        } catch (Exception e){
            System.err.println("Error in GET EXECUTE!");
            e.printStackTrace();
        }
    }

    /**
     * Method that handles the ACK from the server where the GET Execute message was sent to.
     * @param message The ACK message for the GET Execute.
     */
    private void handleGetReply(final Message message){
        try {
            ServerResponseMessageContent getInfo = (ServerResponseMessageContent) message.getContent();

            this.requestState.newClock(new ClientId(getInfo.getServerId().toString()), getInfo.getLogicalClock());

            // Process other requests waiting to enter the queue since we received a new clock
            this.requestState.processWaitingToEnterInQueue();

            if (DEBUG) System.out.println("> GET REPLY from: " + getInfo.getServerId().toString());

            // Handle the get reply message and update the request state by passing the obtained map
            handleGetRequestUpdate(message.getId(), getInfo.getServerId(), (Map<Long, byte[]>) getInfo.getContent());

        } catch (Exception e){
            System.err.println("Error in GET REPLY");
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

            // Update my logical clock
            requestState.updateMyLogicalClock(ScalarLogicalClock.Event.LOCAL, null);

            // Remove the request, it removes it from the currently running requests and from the running get requests.
            requestState.removeGetRequest(messageId);
        } else {
            if (DEBUG) System.out.println("> GET REQUEST is not completed, waiting for response from the contacted servers.");
        }
    }

    /**********************************************************************************************************
     *                                               CLOCKS                                                   *
     **********************************************************************************************************/

    /**
     * Update my clock.
     * @param message The message.
     */
    public void handleClockUpdate(Message message) throws IOException {
        ServerResponseMessageContent clockupdate = (ServerResponseMessageContent) message.getContent();

        if (DEBUG) System.out.println("> Updating my logical clock.");
        this.requestState.newClock(new ClientId(clockupdate.getServerId().toString()), clockupdate.getLogicalClock());

        // Process other requests waiting to enter the queue since we received a new clock
        this.requestState.processWaitingToEnterInQueue();

        // Send my clock to other peers
        sendClockUpdate(serverNetwork.getRemoteServersIds());
    }

    /**
     * Receive a clock update from other peer.
     * @param message The message.
     */
    public void handleClock(Message message) throws IOException {
        ServerRequestMessageContent clock = (ServerRequestMessageContent) message.getContent();

        this.requestState.newClock(new ClientId(clock.getServerId().toString()), clock.getLogicalClock());

        // Process other requests waiting to enter the queue since we received a new clock
        this.requestState.processWaitingToEnterInQueue();
    }

    /**
     * Method that sends clock a updates to a given set of servers.
     * @param remoteServersIdsToSendLock The servers to send clock updates.
     * @throws IOException
     */
    private void sendClockUpdate(List<ServerId> remoteServersIdsToSendLock) throws IOException {
        if (remoteServersIdsToSendLock.size() != 0){
            for (ServerId serverId : remoteServersIdsToSendLock){

                // Create a new message with CLOCK_UPDATE type
                Message clockUpdate = new Message(new MessageId(), RequestType.CLOCK_UPDATE,
                        new ServerRequestMessageContent(
                                localServerId,
                                requestState.getMyLogicalClock(),
                                null
                        )
                );

                if (DEBUG) System.out.println("> Sending CLOCK UPDATE to: " + serverId.toString());

                // Send the message with CLOCK UPDATE to the server
                this.serverNetwork.send(serverId, clockUpdate);

                // Update my logical clock with send event
                this.requestState.updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);
            }
        }
    }

    /**********************************************************************************************************
     *                                          AUXILIARY METHODS                                             *
     **********************************************************************************************************/

    /**
     * Method to remove the connection from the existing connections map.
     * @param clientId The client id to remove the connection.
     */
    public void removeConnection(ClientId clientId){
        this.requestState.removeConnection(clientId);
    }
}
