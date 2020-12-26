import com.sun.source.tree.LiteralTree;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigurationHandler {
    final static String CONFIGURATION_FILE = "/Users/ceciliasoares/Documents/Universidade/4Ano/1semestre/FundamentosSD/fsd-project/src/main/java/servers.yaml";

    public List<String> getServers(String configurationFile) throws FileNotFoundException {
        File initialFile = new File(configurationFile);
        InputStream targetStream = new FileInputStream(initialFile);
        Yaml yaml = new Yaml();
        LinkedHashMap<String, List<String>> configuration = yaml.load(targetStream);
        return configuration.get("servers");
    }

    public List<String> getServers() throws FileNotFoundException {
        return getServers(CONFIGURATION_FILE);
    }
}
