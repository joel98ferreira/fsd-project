package dkvs.shared;

import java.util.HashMap;
import java.util.Map;

public enum RequestType {

    PUT_REQUEST(1), // Request done by client
    PUT_EXECUTE(2), // Execute action on other server, when we know the key is in the message receiver server
    PUT_REPLY(3),
    GET_REQUEST(4), // Request done by client
    GET_EXECUTE(5), // Get execute on the server that will receive this message
    GET_REPLY(6);

    private final int type;

    RequestType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    private final static Map<Integer, RequestType> map = new HashMap<>();

    static {
        for (RequestType type : RequestType.values()) {
            map.put(type.getType(), type);
        }
    }

    static RequestType getMessageType(int type) {
        return map.get(type);
    }
}