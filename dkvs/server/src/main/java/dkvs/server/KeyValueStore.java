package dkvs.server;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {

    // Maps all the keys to values inserted by the client on this server
    private final Map<Long, byte[]> keyVal;

    // Temporary map to hold the previous values when there is a put request holding
    // locks on some keys, when the keys are unlocked (only when the put in all keys of that
    // transaction is successful) then the values are removed from this map and we can use
    // the default map (keyVal). This map also stores a Version enum, that says if the the key
    // in this map is a previous version or a new one, this is useful when receiving a get because
    // we can check if the map has a previous version then we are sure that we can return that value
    // to the client and if it has a new version tag and the client request that key then we don't return
    // that value, because it didn't had a previous version associated with it.
    private final Map<Long, AbstractMap.Entry<Version, byte[]>> temporaryFutureAndPreviousVersions;

    public KeyValueStore() {
        this.keyVal = new ConcurrentHashMap<>();
        this.temporaryFutureAndPreviousVersions = new ConcurrentHashMap<>();
    }

    /**
     * This method validates the GET request and verifies if the values that the client wants
     * are in the previous versions and future versions maps. If a key that the client wants
     * is in the previous version then that will be return. If a key that the client wants is
     * in the previous and future versions maps but is marked with "NEW" then we are sure that
     * that key didn't have an previous version.
     * @param keys The keys to get.
     * @return
     */
    public Map<Long, byte[]> get(Collection<Long> keys){
        Map<Long, byte[]> getResult = new HashMap<>();

        for (Long key : keys){

            // Validate if the key that the client wants is in the previous and future versions map or in the key val store
            if (temporaryFutureAndPreviousVersions.containsKey(key)){
                if (temporaryFutureAndPreviousVersions.get(key).getKey() == Version.NEW) {
                    // We are sure that the this value can only be returned when remove from
                    // this map
                } else {
                    // Else it is a previous version we can return this value to client
                    getResult.put(key, temporaryFutureAndPreviousVersions.get(key).getValue());
                }
            } else {
                if (keyVal.containsKey(key)){
                    getResult.put(key, keyVal.get(key));
                }
            }
        }

        return getResult;
    }

    /**
     * This method inserts all values in the "future version" map, and stores the
     * previous versions of the existing keys in a "previous version" map. If the key
     * doesn't exists in the key val map, then it also stores the new key in the "previous
     * versions", this is useful because when we are receive a GET request we just check if
     * the keySo when
     * we are sure that a transaction is complete then we can remove all of the previous
     * version keys from the "previous version" map. If a transaction goes wrong then we
     * can just rollback and copy all of the previous versions maps.
     * @param values The values to insert.
     */
    public void putPrepare(Map<Long, byte[]> values){
        for (Map.Entry<Long, byte[]>  val : values.entrySet()){
            // If the key has already an associated value, store it's previous version
            if (keyVal.containsKey(val.getKey())){
                this.temporaryFutureAndPreviousVersions.put(val.getKey(), new AbstractMap.SimpleEntry<>(Version.PREVIOUS, this.keyVal.get(val.getKey())));
                keyVal.replace(val.getKey(), val.getValue());
            }else { // If no value is associated with the key, then insert a new entry
                this.temporaryFutureAndPreviousVersions.put(val.getKey(), new AbstractMap.SimpleEntry<>(Version.NEW, val.getValue()));
                keyVal.put(val.getKey(), val.getValue());
            }
        }
    }

    /**
     * Rollback to the previous version.
     * @param keys The keys to rollback.
     */
    public void rollbackPut(Collection<Long> keys){
        for (Long key : keys){
            if (this.temporaryFutureAndPreviousVersions.containsKey(key)){
                if (this.temporaryFutureAndPreviousVersions.get(key).getKey() == Version.PREVIOUS) {
                    this.keyVal.replace(key, this.temporaryFutureAndPreviousVersions.get(key).getValue());
                } else { // NEW VERSION, Remove completely from the key val store
                    this.keyVal.remove(key);
                }
                this.temporaryFutureAndPreviousVersions.remove(key);
            }
        }
    }

    /**
     * Commit the put "transaction" and make all values available in the key value store
     * @param keys The keys to commit.
     */
    public void commitPut(Collection<Long> keys){
        for (Long key : keys){
            this.temporaryFutureAndPreviousVersions.remove(key);
        }
    }

    public enum Version {

        PREVIOUS(1),  // Previous version
        NEW(2);       // New version

        private final int version;

        Version(int version) {
            this.version = version;
        }

        public int getType() {
            return version;
        }

        private final static Map<Integer, Version> map = new HashMap<>();

        static {
            for (Version type : Version.values()) {
                map.put(type.getType(), type);
            }
        }

        static Version getVersion(int type) {
            return map.get(type);
        }
    }
}
