package dkvs.client;

import dkvs.shared.MessageId;

import java.util.Collection;
import java.util.Map;

public class RequestState {

    // Map where the value is the Message ID and the value is all the requested keys in the get request
    private Map<MessageId, Collection<Long>> requestedKeysByRequestId;


}
