import java.io.FileNotFoundException;
import java.util.*;

//algoritmo de consistent hashing retirado de https://programmer.help/blogs/consistency-hash-algorithm-principle-and-java-implementation.html
/**
 * Consistency Hash algorithm without virtual nodes
 */
public class ConsistentHashing {
    //List of servers to be added to the Hash ring
    private static List<String> servers;

    static {
        ConfigurationHandler configuration = new ConfigurationHandler();
        try {
            servers = configuration.getServers();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    //key represents the hash value of the server and value represents the server
    private static SortedMap<Integer, String> sortedMap = new TreeMap<Integer, String>();

    //Program initialization, put all servers into sortedMap
    static {
        for (int i = 0; i < servers.size(); i++) {
            int hash = getHash(servers.get(i));
            System.out.println("[" + servers.get(i) + "] joins the collection, its hash value is: " + hash);
            sortedMap.put(hash, servers.get(i));
        }
        System.out.println();
    }

    //Get the node that should be routed to
    public static String getServer(String key) {
        //Get the hash value of the key
        int hash = getHash(key);
        //Get all Map s that are larger than the Hash value
        SortedMap<Integer, String> subMap = sortedMap.tailMap(hash);
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

    /*
    Using FNV1_32_HASH algorithm to calculate the Hash value of the server,
    there is no need to rewrite hashCode method, the final effect is no difference.
     */
    public static int getHash(String str) {
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

    public Map<String, Map<Long, byte[]>> mappingServer(Map<Long, byte[]> clientPut){

        //mapping all the servers with the keys-values in clientPut
         Map<String, Map<Long, byte[]>> serversMapping = new HashMap<String, Map<Long, byte[]>>();

        for( Map.Entry<Long, byte[]> entry: clientPut.entrySet()) {
            String server = getServer(entry.getKey().toString());
            Map<Long, byte[]> map = serversMapping.get(server);
            if (map == null) {
                map = new HashMap<Long, byte[]>();
                serversMapping.put(server, map);
            }
            map.put(entry.getKey(), entry.getValue());
        }

        return serversMapping;
    }

}