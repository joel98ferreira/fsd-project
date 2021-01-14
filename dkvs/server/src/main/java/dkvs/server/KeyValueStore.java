package dkvs.server;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {

    // Maps all the keys to values inserted by the client on this server
    private final Map<Long, byte[]> keyVal;

    // Temporary map to hold the previous values when there is a put request holding
    // locks on some keys, when the keys are unlocked (only when the put in all keys of that
    // transaction is successful) then the values are removed from this map and we can use
    // the default map (keyVal). If an error occurs then the previous versions are replaced.
    private final Map<Long, byte[]> temporaryPreviousVersions;

    public KeyValueStore() {
        this.keyVal = new HashMap<>();
        this.temporaryPreviousVersions = new HashMap<>();
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
