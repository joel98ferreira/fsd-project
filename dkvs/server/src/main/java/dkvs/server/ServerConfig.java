package dkvs.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import dkvs.server.identity.ServerId;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class ServerConfig {

    private final ServerId localServerId;

    public ServerConfig(ServerId localServerId){
        this.localServerId = Objects.requireNonNull(localServerId);

    }

    public static ServerConfig parseYamlFile(Path filePath) throws IOException
    {
        return parseYaml(Files.readString(filePath));
    }

    public static ServerConfig parseServerConfigFileYaml(Path configFilePath) {
        // parse yaml

        final var parser = new Yaml(new Constructor(YamlRoot.class));
        final var root = parser.< YamlRoot >load(yaml);

        // convert to config

        return new ServerConfig(
                new ServerId(root.localServer.id),
                root.localServer.port,
                root.remoteServers
                        .stream()
                        .collect(Collectors.toMap(
                                s -> new ServerId(s.id),
                                s -> Address.from(s.host, s.port)
                        ))
        );
    }

    private static class YamlRoot
    {
        public YamlLocalServer localServer;
        public List< YamlRemoteServer > remoteServers;
    }

    private static class YamlLocalServer
    {
        public int id;
        public int port;
    }

    private static class YamlRemoteServer
    {
        public String host;
        public int port;
        public int id;
    }
}
