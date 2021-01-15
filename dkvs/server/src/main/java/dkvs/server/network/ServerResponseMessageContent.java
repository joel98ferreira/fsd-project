package dkvs.server.network;

import dkvs.server.identity.ServerId;

public class ServerResponseMessageContent {

    // Used to identify which server sent the message
    private final ServerId serverId;

    // Logical Clock
    private final ScalarLogicalClock logicalClock;

    // Status code of the message
    private final int statusCode;

    // Content object
    private final Object object;

    public ServerResponseMessageContent(ServerId serverId, ScalarLogicalClock scalarLogicalClock, int statusCode, Object object) {
        this.serverId = serverId;
        this.statusCode = statusCode;
        this.logicalClock = scalarLogicalClock;
        this.object = object;
    }

    public ServerId getServerId() {
        return serverId;
    }

    public ScalarLogicalClock getLogicalClock(){
        return this.logicalClock;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Object getContent() {
        return object;
    }
}
