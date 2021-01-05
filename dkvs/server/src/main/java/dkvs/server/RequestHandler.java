package dkvs.server;

import dkvs.server.identity.ServerId;
import dkvs.shared.Message;
import dkvs.shared.RequestType;

import java.util.Map;
import java.util.Objects;

public class RequestHandler {

    private final ServerId serverId;
    private final ConsistencyHash consistencyHash;

    public RequestHandler(ServerConfig config) {
        this.serverId = Objects.requireNonNull(config.getLocalServerId());
        this.consistencyHash = new ConsistencyHash(config);
    }

    public void handleMessage(Message message){
        switch (message.getType()){
            case PUT_REQUEST: // Request received from a client
                handlePutRequest(message);
                break;
            case PUT_EXECUTE: // Execution received from a known peer (server)
                handlePutExecute(message);
                break;
            case GET_REQUEST: // Request received from a client
                handleGetRequest(message);
                break;
            case GET_EXECUTE: // Execution received from a known peer (server)
                handleGetExecute(message);
                break;
            case PUT_REPLY:
                break;
            case GET_REPLY:
                break;
            default:
                // Unknown message
                // TODO: Throw exception
                break;
        }
    }

    public void handlePutRequest(Message message){
        try {
            Map<Long, byte[]> values = (Map<Long, byte[]>) message.getContent();

            // Map the put by server
            Map<ServerId, Map<Long, byte[]>> mappedByServerPut = consistencyHash.mapServerPutRequest(values);

            // Verify if any put request is to the current server
            if (mappedByServerPut.containsKey(this.serverId)){
                Map<Long, byte[]> myPutReq = mappedByServerPut.get(this.serverId);


            }


            // Send the request to each server TODO: concorrentemente?

            for (Map.Entry<ServerId, Map<Long, byte[]>> serverPutExecute : mappedByServerPut.entrySet()){
                // Create a new message with PUT_EXECUTE type

                // Mandar relógios lógicos
                Message putExecute = new Message(RequestType.PUT_EXECUTE, serverPutExecute.getValue());
            }


        } catch (Exception e){
            // TODO: Thrown unknown request
        }

    }

    public void handlePutExecute(Message message){
        try {
            Map<Long, byte[]> values = (Map<Long, byte[]>) message.getContent();

            // Map the put by server
            Map<ServerId, Map<Long, byte[]>> mappedByServerPut = consistencyHash.mapServerPutRequest(values);



        } catch (Exception e){
            // TODO: Thrown unknown request
        }

    }

    public void handleGetRequest(Message message){

    }

    public void handleGetExecute(Message message){

    }
}
