package dkvs.server;

import dkvs.server.identity.*;

import java.util.*;

/**
 * Consistency Hash algorithm without virtual nodes, with variants implemented by our group
 * Font: https://programmer.help/blogs/consistency-hash-algorithm-principle-and-java-implementation.html
 */
public class ConsistencyHash {

    // Key represents the hash value of the server and value represents the server id
    private static final SortedMap<Integer, ServerId> sortedMap = new TreeMap<>();

    public ConsistencyHash(ServerConfig serverConfig){

        // Initialize the Sorted Map
        for (Map.Entry<ServerId, ServerAddress> server : serverConfig.getRemoteServers().entrySet()) {
            String serverAddressString = server.getValue().toString();
            int hash = getHash(serverAddressString);
            System.out.println("[" + serverAddressString + "] joins the collection, its hash value is: " + hash);
            sortedMap.put(hash, server.getKey());
        }
        // Insert in the sorted map the local server
        int hashLocalServer = getHash(serverConfig.getLocalServerAddress().toString());
        sortedMap.put(hashLocalServer, serverConfig.getLocalServerId());
    }

    public <K, V> Map<ServerId, Map<K, V>> mapServerPutRequest(Map<K, V> map){

        // Mapping all the servers with the keys-values put
        Map<ServerId, Map<K, V>> serversMapping = new HashMap<>();

        for(Map.Entry<K, V> entry: map.entrySet()) {
            ServerId server = getServer(entry.getKey().toString());
            Map<K, V> storedMap = serversMapping.get(server);
            if (!serversMapping.containsKey(server)) {
                storedMap = new HashMap<K, V>();
                serversMapping.put(server, storedMap);
            }
            storedMap.put(entry.getKey(), entry.getValue());
        }

        return serversMapping;
    }

    public <K> Map<ServerId, Collection<K>> mapServerGetRequest(Collection<K> keys){

        // Map server id with a collection of keys to obtain from a server
        Map<ServerId, Collection<K>> serversMapping = new HashMap<>();

        for (K key : keys){
            ServerId server = getServer(key.toString());

            if (!serversMapping.containsKey(server)){
                serversMapping.put(server, new ArrayList<>());
            }else{
                Collection<K> storedKeys = serversMapping.get(server);
                storedKeys.add(key);
            }
        }
        return serversMapping;
    }

    // Get the node where the request should be routed to
    public static ServerId getServer(String key) {
        //Get the hash value of the key
        int hash = getHash(key);
        //Get all Map s that are larger than the Hash value
        SortedMap<Integer, ServerId> subMap = sortedMap.tailMap(hash);
        if (subMap.isEmpty()) {
            //If there is no one larger than the hash value of the key, start with the first node
            Integer i = sortedMap.firstKey();
            //Return to the corresponding server
            return sortedMap.get(i);
        } else {
            //The first Key is the nearest node clockwise past the node.
            Integer i = subMap.firstKey();
            //Return to the corresponding server
            return subMap.get(i);
        }
    }

    /**
     * Using FNV1_32_HASH algorithm to calculate the Hash value of the server,
     * there is no need to rewrite hashCode method, the final effect is no difference.
     * @param str Server address
     */
    private static int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        // If the calculated value is negative, take its absolute value.
        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }
}
