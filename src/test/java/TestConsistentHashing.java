import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class TestConsistentHashing {

    @Test
    public void getServer() {
       ConsistentHashing ch = new ConsistentHashing();
       String node = ch.getServer("12");
       int nodeHash = ch.getHash("192.168.0.1:111");
       int keyhash = ch.getHash("12");
       Assert.assertEquals(1796605027, keyhash);
       Assert.assertEquals("192.168.0.1:111", node);
       Assert.assertEquals(8518713, nodeHash);
    }

    @Test
    public void getServer2() {
        ConsistentHashing ch = new ConsistentHashing();
        String node = ch.getServer("1000");
        int nodeHash = ch.getHash("192.168.0.3:111");
        int keyhash = ch.getHash("1000");
        Assert.assertEquals(983876293, keyhash);
        Assert.assertEquals("192.168.0.3:111", node);
        Assert.assertEquals(1171828661, nodeHash);
    }


    @Test
    public void mappingServerTest() {
        Map<Long, byte[]> clientPut = new HashMap<Long, byte[]>();

        final ConsistentHashing ch = new ConsistentHashing();

        final Long l1 = 15000000000L;
        final Long l2 = 34340000023L;
        final byte b[] = "Bytes".getBytes(StandardCharsets.UTF_8);
        clientPut.put(l1, b);
        clientPut.put(l2, b);

        Map<String, Map<Long, byte[]>> result = ch.mappingServer(clientPut);

        String key1 = l1.toString();
        String key2 = l2.toString();


        String node1 = ch.getServer(key1);
        String node2 = ch.getServer(key2);

        int nodeHash1 = ch.getHash("192.168.0.3:111");
        int nodeHash2 = ch.getHash("192.168.0.0:111");

        int keyhash1 = ch.getHash(l1.toString());
        int keyhash2 = ch.getHash(l2.toString());

        // result = {192.168.0.3:111={34340000023=[B@d2cc05a}, 192.168.0.0:111={15000000000=[B@d2cc05a}}
        //Assert.assertEquals({192.168.0.3:111={34340000023=[B@d2cc05a}, 192.168.0.0:111={15000000000=[B@d2cc05a}}, result);
        Assert.assertEquals(391410978, keyhash1);
        Assert.assertEquals(1093485035, keyhash2);


        Assert.assertEquals("192.168.0.0:111", node1);
        Assert.assertEquals("192.168.0.3:111", node2);

        Assert.assertEquals(1171828661, nodeHash1);
        Assert.assertEquals(575774686, nodeHash2);

    }
}
