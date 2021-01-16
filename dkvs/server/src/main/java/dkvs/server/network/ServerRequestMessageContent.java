package dkvs.server.network;

import dkvs.server.identity.ServerId;

public class ServerRequestMessageContent implements Comparable<ServerRequestMessageContent> {

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

    @Override
    public int compareTo(ServerRequestMessageContent msg) {
        int cmp = this.logicalClock.compareTo(msg.logicalClock);
        if (cmp == 0) {
            cmp = this.serverId.compareTo(msg.serverId);
        }
        return cmp;
    }
}
