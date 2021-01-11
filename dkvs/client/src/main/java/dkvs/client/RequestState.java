package dkvs.client;

import java.util.Collection;
import java.util.Map;

public class RequestState {

    // Map where the value is the request ID and the value is all the requested keys in the get request
    private Map<String, Collection<Long>> requestedKeysByRequestId;


}
