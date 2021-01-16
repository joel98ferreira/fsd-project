package dkvs.client;

import dkvs.shared.MessageId;

import java.util.HashMap;
import java.util.Map;

public class RequestState {

    private final Map<MessageId, Boolean> requests;

    public RequestState() {
        this.requests = new HashMap<>();
    }

    public void addRequest(MessageId messageId){
        this.requests.put(messageId, false);
    }

    public boolean getRequestStatus(MessageId messageId){
        return this.requests.get(messageId);
    }

    public void removeRequest(MessageId messageId){
        this.requests.remove(messageId);
    }
}
