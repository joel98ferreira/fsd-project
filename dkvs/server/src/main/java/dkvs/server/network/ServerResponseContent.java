package dkvs.server.network;

import dkvs.server.identity.ServerId;

import java.io.IOException;
import java.io.Serializable;

public class ServerResponseContent implements Serializable {

    private static final long serialVersionUID = 7526472295622776147L;

    // Used to identify which server sent the message
    private ServerId serverId;

    // Status code of the message
    private int statusCode;

    // Content object
    private Object object;

    public ServerResponseContent(ServerId serverId, int statusCode, Object object) {
        this.serverId = serverId;
        this.statusCode = statusCode;
        this.object = object;
    }

    public ServerId getServerId() {
        return serverId;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Object getContent() {
        return object;
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
        stream.writeObject(serverId);
        stream.writeInt(statusCode);
        stream.writeObject(object);
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
        serverId = (ServerId) stream.readObject();
        statusCode = stream.readInt();
        object = stream.readObject();
    }
}
