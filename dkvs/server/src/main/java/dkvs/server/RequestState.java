package dkvs.server;

import dkvs.server.identity.ClientId;
import dkvs.server.identity.ServerId;
import dkvs.server.network.ScalarLogicalClock;
import dkvs.server.network.ServerNetwork;
import dkvs.server.network.ServerRequestMessageContent;
import dkvs.server.network.ServerResponseMessageContent;
import dkvs.shared.Message;
import dkvs.shared.MessageId;
import dkvs.shared.Network;
import dkvs.shared.RequestType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class RequestState {

    // This queue represents the priority of the currently running put requests, status could be initial
    // (acquiring locks), waiting to acquire locks or waiting, this "queue" is sorted by the logical clocks.
    private final ConcurrentSkipListSet<AbstractMap.SimpleEntry<Message, Status>> priorityQueue;

    // Queue containing the messages that tried to be inserted in the queue but the received clock is higher
    // or equal than the minimum of all processes, this is sorted by the logical clocks, so the first element
    // is always the element with the less clock.
    private final ConcurrentSkipListSet<Message> waitingToBeInsertedInQeue;

    // Map where the key is the key corresponding to this server and the boolean representing if this key
    // is locked or no
    private final Map<Long, Boolean> locksOnKeysInThisServer;

    // This map represents if it has already received the reply with the write confirmation for a key.
    // Even if it receives the write confirmation in one key the lock is only fully released when the transaction
    // that has the lock in that key releases all locks in it's keys, so we can ensure atomicity.
    private final Map<MessageId, Map<Long, Boolean>> transactionConfirmations;

    // Represents this server logical clock
    private final ScalarLogicalClock myLogicalClock;

    // Represents the remote servers max clocks
    private final Map<ClientId, ScalarLogicalClock> logicalClocksByClientId;

    // This map represents the active connections, that make requests to the server. The id is the
    // client UUID and the value is the network connecting the server with the client.
    private final Map<ClientId, Network> connections;

    // This map represents the currently running get requests, where the Id is the request UUID,
    // and the value is the map that is being built based on the answer received from the other servers
    private final Map<MessageId, Map<Long, byte[]>> getRequests;

    // This map represents the currently running requests (GET), where the Id is the request UUID,
    // and the value is pair that contains in the left the client uuid of the original client, the one
    // that initially made the request, at the right a map containing the servers which we need to
    // contact in order to return a response for the client as value we have a boolean that represents
    // if we already received a response message from that server or no. We opted for this design,
    // since when we receive replies from contacted servers we just receive in the response the request
    // UUID and not the client uuid that originally requested it.
    private final Map<MessageId, Map.Entry<ClientId, Map<ServerId, Boolean>>> serverRequests;

    // The Server Network
    private final ServerNetwork serverNetwork;

    // Key Value Store
    private final KeyValueStore keyValueStore;

    public RequestState(ServerConfig config, ServerNetwork serverNetwork, KeyValueStore keyValueStore) {

        this.serverNetwork = Objects.requireNonNull(serverNetwork);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);

        this.logicalClocksByClientId = new ConcurrentHashMap<>();

        // Obtain the set of existing processes in the network and initialize the clocks
        Set<ServerId> remoteServers = config.getRemoteServers().keySet();
        remoteServers.forEach(server -> this.logicalClocksByClientId.put(new ClientId(server.toString()), new ScalarLogicalClock()));

        this.priorityQueue = new ConcurrentSkipListSet<>(new PriorityQueueMessageComparator());
        this.waitingToBeInsertedInQeue = new ConcurrentSkipListSet<>(new MessageComparator());
        this.transactionConfirmations = new ConcurrentHashMap<>();
        this.locksOnKeysInThisServer = new ConcurrentHashMap<>();

        this.myLogicalClock = new ScalarLogicalClock();
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
     * Method used when receiving a new clock update.
     * @param clientId The client id that owns the clock.
     * @param receivedClock The received clock
     */
    public void newClock(ClientId clientId, ScalarLogicalClock receivedClock){
        if (logicalClocksByClientId.containsKey(clientId)){
            logicalClocksByClientId.replace(clientId, receivedClock);
        } else {
            logicalClocksByClientId.put(clientId, receivedClock);
        }

        // Update my logical clock
        updateMyLogicalClock(ScalarLogicalClock.Event.RECEIVE, receivedClock);
    }

    /**
     * Method that given a received clock validates if it can be put into the queue of locks.
     * @param receivedClock The received clock.
     * @return
     */
    public boolean isClockValidToInsertInQueue(ScalarLogicalClock receivedClock){
        int minClocks = myLogicalClock.getCounter();

        for (ScalarLogicalClock clock : logicalClocksByClientId.values()){
            minClocks = Math.min(minClocks, clock.getCounter());
        }

        // Verify if the received clock is less than the minimum of the clocks for all processes
        return receivedClock.getCounter() < minClocks;
    }

    /**
     * Method that inserts a message in a the queue, if it haves dependencies in other keys
     * then it just inserts the key in the queue with status waiting, otherwise stays in the
     * queue with status running.
     * @param message  The message id.
     * @param keys The keys
     */
    public synchronized Status insertInQueue(Message message, Collection<Long> keys){

        Map<Long, Boolean> keysWriteConfirmation = new ConcurrentHashMap<>();
        keys.forEach(key -> keysWriteConfirmation.put(key, false));

        this.transactionConfirmations.put(message.getId(), keysWriteConfirmation);

        boolean haveDependenciesInQueue = false;

        for (AbstractMap.SimpleEntry<Message, Status> t : priorityQueue){
            if (t.getValue() == Status.WAITING){
                for (Long keyToAcquireLock : keys) {
                    if (transactionConfirmations.get(t.getKey().getId()).containsKey(keyToAcquireLock) ||
                            (locksOnKeysInThisServer.containsKey(keyToAcquireLock) &&
                                    locksOnKeysInThisServer.get(keyToAcquireLock) != null &&
                                    locksOnKeysInThisServer.get(keyToAcquireLock).equals(true)
                            )
                    ){
                        haveDependenciesInQueue = true;
                    }
                }
            }
        }

        if (haveDependenciesInQueue) {
            System.out.println("> Had dependencies in the request, waiting for lock releases....");
            this.priorityQueue.add(new AbstractMap.SimpleEntry<>(message, Status.WAITING));

            return Status.WAITING;
        } else {
            // Acquire locks
            for (Long keyToLock : keys){
                this.locksOnKeysInThisServer.put(keyToLock, true);
            }

            System.out.println("> No dependencies acquiring locks....");

            this.priorityQueue.add(new AbstractMap.SimpleEntry<>(message, Status.RUNNING));

            return Status.RUNNING;
        }
    }

    /**
     * This method receives a message that don't have the clocks valid yet and insert
     * this message in the queue to enter the transactions queue once the clock is satisfied.
     * @param message Message to insert in queue.
     */
    public void insertInWaitingToEnterQueue(Message message){
        this.waitingToBeInsertedInQeue.add(message);
    }

    /**
     * Method that once we receive a logical clock from other process verifies if a message
     * currently in a waiting to enter the queue can be moved to the queue.
     */
    public void processWaitingToEnterInQueue() throws IOException {

        for (Message m : waitingToBeInsertedInQeue){
            ServerRequestMessageContent requestMessageContent = (ServerRequestMessageContent) m.getContent();
            processToEnterInQueue(m, requestMessageContent);
        }
    }

    /**
     * Process one process to enter the queue.
     * @param m The message.
     * @param requestMessageContent The message content.
     * @throws IOException
     */
    private void processToEnterInQueue(Message m, ServerRequestMessageContent requestMessageContent) throws IOException {
        if (isClockValidToInsertInQueue(requestMessageContent.getLogicalClock())){
            System.out.println("> Processing the requests in the waiting to enter queue!");

            AbstractMap.SimpleEntry<Map<Long, byte[]>, Collection<Long>> putExecuteInfo = (AbstractMap.SimpleEntry<Map<Long, byte[]>, Collection<Long>>) requestMessageContent.getContent();

            // Obtain all keys of the transaction
            Collection<Long> lockedKeys = putExecuteInfo.getValue();
            lockedKeys.addAll(putExecuteInfo.getKey().keySet());

            // Insert this request in the priority queue
            Status status = insertInQueue(m, lockedKeys);

            if (status == RequestState.Status.WAITING){
                System.out.println("> Found dependencies, waiting to obtain lock in some keys...");
                return;
            } else {
                System.out.println("> Obtained all locks with success! Running PUT EXECUTE!");
            }

            // Update the values in the Key Value Store and save the previous ones
            this.keyValueStore.putPrepare(putExecuteInfo.getKey());
            System.out.println("> PUT Prepare is done.");

            // Update the received write confirmation
            RequestState.Status s = receivedWriteConfirmation(m.getId(), putExecuteInfo.getKey().keySet());

            // Send message to all servers
            System.out.println("> Sending PUT REPLY (implicit unlock) to everyone.");
            sendPutReplyToAll(m.getId(), putExecuteInfo.getKey().keySet());

            if (s == RequestState.Status.COMPLETED){
                processCompletedRequest(m.getId());
            }

            // Remove from the waiting to enter in queue
            waitingToBeInsertedInQeue.removeIf(ms -> ms.getId().equals(m.getId()));
        }
    }

    /**
     * Method that once receives a write confirmation with put prepare on some keys in a
     * running transaction marks them as done, if it completes then sends result to client.
     * @param messageId The messageId of the transaction.
     * @param keys The keys written.
     */
    public synchronized Status receivedWriteConfirmation(MessageId messageId, Collection<Long> keys){

        if (this.transactionConfirmations.containsKey(messageId)){
            for (Long k : keys){
                if (transactionConfirmations.get(messageId).containsKey(k)){
                    this.transactionConfirmations.get(messageId).replace(k, true);
                }
            }

            // If is completed
            if (!this.transactionConfirmations.get(messageId).containsValue(false)){
                return Status.COMPLETED;
            } else {
                return Status.NOT_COMPLETED;
            }
        }
        return Status.ERROR;
    }

    /**
     * Method that confirms a message, as unlocked and processes the messages waiting in the queue.
     * @param messageId The id that identifies the "transaction".
     */
    public synchronized void unlockKeys(MessageId messageId){
        Collection<Long> keysToUnlock = this.transactionConfirmations.get(messageId).keySet();

        for (Long k : keysToUnlock){
            if (this.locksOnKeysInThisServer.containsKey(k))
                this.locksOnKeysInThisServer.replace(k, false);
        }

        this.transactionConfirmations.remove(messageId);
        this.priorityQueue.removeIf(s -> s.getKey().getId().equals(messageId));
    }

    /**
     * Method that processes the next message in the queue.
     */
    private void processNextMessageToAcquireLocks() throws IOException {
        for (AbstractMap.SimpleEntry<Message, Status> t : priorityQueue){
            if (t.getValue() == Status.WAITING){
                ServerRequestMessageContent requestMessageContent = (ServerRequestMessageContent) t.getKey().getContent();
                AbstractMap.SimpleEntry<Map<Long, byte[]>, Collection<Long>> putInfo = (AbstractMap.SimpleEntry<Map<Long, byte[]>, Collection<Long>>) requestMessageContent.getContent();
                Collection<Long> keysToAcquireLock = putInfo.getKey().keySet();

                boolean canBeProcessed = true;

                for (Long keyToAcquireLock : keysToAcquireLock) {
                    if (locksOnKeysInThisServer.containsKey(keyToAcquireLock) &&
                            locksOnKeysInThisServer.get(keyToAcquireLock) != null &&
                            locksOnKeysInThisServer.get(keyToAcquireLock).equals(true)) {

                        canBeProcessed = false;
                    }
                }

                if (canBeProcessed){
                    // Insert with status running
                    priorityQueue.add(new AbstractMap.SimpleEntry<>(t.getKey(), Status.RUNNING));

                    // Remove from the queue
                    priorityQueue.removeIf(s -> s.getKey().getId().equals(t.getKey().getId()) && s.getValue() == Status.WAITING);

                    // Acquire locks
                    for (Long keyToLock : keysToAcquireLock){
                        this.locksOnKeysInThisServer.put(keyToLock, true);
                    }

                    // Prepare the PUT
                    keyValueStore.putPrepare(putInfo.getKey());

                    // Mark as done
                    Status s = receivedWriteConfirmation(t.getKey().getId(), putInfo.getKey().keySet());

                    // Send message to all servers
                    this.sendPutReplyToAll(t.getKey().getId(), putInfo.getKey().keySet());

                    if (s == Status.COMPLETED){
                        processCompletedRequest(t.getKey().getId());
                    } else if (s == Status.NOT_COMPLETED){
                        return;
                    }
                }
            }
        }
    }

    /**
     * Completed request process, do the put commit on the keys.
     * @param messageId Message id to commit.
     */
    public void processCompletedRequest(MessageId messageId) throws IOException {

        // Commit PUT
        this.keyValueStore.commitPut(this.transactionConfirmations.get(messageId).keySet());

        // Verify if I have the client who originally made this request or if it was just a put reply
        if (this.serverRequests.containsKey(messageId)){

            System.out.println("> PUT REQUEST is complete, sending result to client.");

            // Send a success message to the client
            Message response = new Message(messageId, RequestType.PUT_REPLY, 200);
            sendOriginalRequestResponse(messageId, response);

            // Remove the request
            removeRequest(messageId);
        }

        // Unlock keys
        unlockKeys(messageId);

        // Process next request in queue
        processNextMessageToAcquireLocks();
    }

    /**
     * Method that given a message id sends a put reply to all other servers
     * @param messageId Message id.
     * @param putPrepareKeys The keys that were written in this server.
     */
    public void sendPutReplyToAll(MessageId messageId, Collection<Long> putPrepareKeys) throws IOException {
        // Send put reply to all other servers
        for (ServerId serverId : serverNetwork.getRemoteServersIds()){

            Message putReply = new Message(
                    messageId,
                    RequestType.PUT_REPLY,
                    new ServerResponseMessageContent(
                            this.serverNetwork.getLocalServerId(),
                            myLogicalClock,
                            200,
                            putPrepareKeys
                    )
            );

            // Send the put reply to server
            serverNetwork.send(serverId, putReply);

            // Update logical clock
            updateMyLogicalClock(ScalarLogicalClock.Event.SEND, null);
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
     * Method that returns my current clock
     */
    public ScalarLogicalClock getMyLogicalClock(){
        return this.myLogicalClock;
    }

    /**
     * Method that given an event updates my logical clock.
     * @param event The event.
     * @param receivedClock Only used in a receive event.
     */
    public void updateMyLogicalClock(ScalarLogicalClock.Event event, ScalarLogicalClock receivedClock){
        switch (event){
            case SEND:
                this.myLogicalClock.sendEvent();
                System.out.println("> Send Event, My logical clock is now: " + myLogicalClock.getCounter());
                break;
            case LOCAL:
                this.myLogicalClock.localEvent();
                System.out.println("> Local Event, My logical clock is now: " + myLogicalClock.getCounter());
                break;
            case RECEIVE:
                this.myLogicalClock.receiveEvent(receivedClock);
                System.out.println("> Receive Event, My logical clock is now: " + myLogicalClock.getCounter());
                break;
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

    public enum Status {

        INITIAL(1),  // Initial status to obtain locks on keys
        RUNNING(2),  // Running
        WAITING(3),  // Waiting to obtain locks in some keys

        COMPLETED(4),       // Is completed
        NOT_COMPLETED(5),   // Still running but not completed
        ERROR(6);           // Error

        private final int status;

        Status(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }

        private final static Map<Integer, Status> map = new HashMap<>();

        static {
            for (Status s : Status.values()) {
                map.put(s.getStatus(), s);
            }
        }

        static Status getMessageType(int status) {
            return map.get(status);
        }
    }

    private static class PriorityQueueMessageComparator implements Comparator<AbstractMap.SimpleEntry<Message, Status>> {
        @Override
        public int compare(AbstractMap.SimpleEntry<Message, Status> o1, AbstractMap.SimpleEntry<Message, Status> o2) {
            ServerRequestMessageContent msg1 = (ServerRequestMessageContent) o1.getKey().getContent();
            ServerRequestMessageContent msg2 = (ServerRequestMessageContent) o2.getKey().getContent();
            return msg1.compareTo(msg2);
        }
    };


    private static class MessageComparator implements Comparator<Message> {
        @Override
        public int compare(Message o1, Message o2) {
            ServerRequestMessageContent msg1 = (ServerRequestMessageContent) o1.getContent();
            ServerRequestMessageContent msg2 = (ServerRequestMessageContent) o2.getContent();
            return msg1.compareTo(msg2);
        }
    };
}
