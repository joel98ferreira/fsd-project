package dkvs.server;

import dkvs.server.identity.ServerAddress;
import dkvs.server.identity.ServerId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {

    // Maps all the keys to values inserted by the client on this server
    private Map<Long, byte[]> keyVal;

    public KeyValueStore() {
        this.keyVal = new HashMap<>();
    }

    private long latestTimestamp;

    public Map<Long, byte[]> get(Collection<Long> keys){
        Map<Long, byte[]> getResult = new HashMap<>();

        for (Long key : keys){
            if (keyVal.containsKey(key)){
                getResult.put(key, keyVal.get(key));
            }
        }

        return getResult;
    }

    public void put(Map<Long, byte[]> values){

        for (Map.Entry<Long, byte[]>  val : values.entrySet()){

            // If the key has already an associated value, then replace it
            if (keyVal.containsKey(val.getKey())){
                keyVal.replace(val.getKey(), val.getValue());
            }else { // If no value is associated with the key, then insert a new entry
                keyVal.put(val.getKey(), val.getValue());
            }
        }
    }

}
