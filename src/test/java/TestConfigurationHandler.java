import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class TestConfigurationHandler {
    @Test
    public void loadYamlTest() throws FileNotFoundException {
        ConfigurationHandler cf = new ConfigurationHandler();
        List<String> servers = cf.getServers();
        Assert.assertEquals(5, servers.size());
    }

    @Test
    public void contentYamlTest() throws FileNotFoundException {
        ConfigurationHandler cf = new ConfigurationHandler();
        List<String> servers = cf.getServers();
        List<String> serversYaml = new ArrayList<String>();
        serversYaml.add("192.168.0.0:111");
        serversYaml.add("192.168.0.1:111");
        serversYaml.add("192.168.0.2:111");
        serversYaml.add("192.168.0.3:111");
        serversYaml.add("192.168.0.4:111");
        Assert.assertEquals(serversYaml, servers);
    }
}
