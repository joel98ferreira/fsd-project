package dkvs.server.network;

import dkvs.server.identity.ServerId;

public class ServerRequestMessageContent {

    // Server Id of the server who is sending the request
    private final ServerId serverId;

    // Logical Clock
    private final ScalarLogicalClock logicalClock;

    // Content object
    private final Object object;

    public ServerRequestMessageContent(ServerId serverId, ScalarLogicalClock logicalClock, Object object) {
        this.serverId = serverId;
        this.logicalClock = logicalClock;
        this.object = object;
    }

    public ServerId getServerId(){
        return this.serverId;
    }

    public ScalarLogicalClock getLogicalClock(){
        return this.logicalClock;
    }

    public Object getContent() {
        return object;
    }
}
