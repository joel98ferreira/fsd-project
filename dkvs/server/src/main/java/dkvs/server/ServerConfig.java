package dkvs.server;

import dkvs.server.identity.*;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class ServerConfig {

    private final ServerId localServerId;
    private final ServerAddress localServerAddress;

    private final Map<ServerId, ServerAddress> remoteServersById;

    public ServerConfig(ServerId localServerId, ServerAddress localServerAddress, Map<ServerId, ServerAddress> remoteServers){
        this.localServerId = Objects.requireNonNull(localServerId);
        this.localServerAddress = localServerAddress;
        this.remoteServersById = new HashMap<>(remoteServers);
    }

    public static ServerConfig parseServerConfigFileYaml(Path configFilePath) throws FileNotFoundException {

        // Parse the YAML file
        final Yaml parser = new Yaml(new Constructor(YamlServersFile.class));
        final YamlServersFile serversFile = parser.load(new FileInputStream(new File(configFilePath.toUri())));

        // Create a new server config
        return new ServerConfig(
                new ServerId(serversFile.localServer.id),
                new ServerAddress("localhost", serversFile.localServer.port),
                serversFile.remoteServers.stream().collect(Collectors.toMap(
                        s -> new ServerId(s.id),
                        s -> new ServerAddress(s.host, s.port)
                ))
        );
    }

    public ServerId getLocalServerId() {
        return this.localServerId;
    }

    public ServerAddress getLocalServerAddress() {
        return this.localServerAddress;
    }

    public Map<ServerId, ServerAddress> getRemoteServers() {
        return Collections.unmodifiableMap(remoteServersById);
    }

    public ServerAddress getRemoteServerAddressByServerId(ServerId serverId){
        return this.remoteServersById.get(serverId);
    }

    private static class YamlServersFile {
        public YamlLocalServer localServer;
        public List<YamlRemoteServer> remoteServers;
    }

    private static class YamlLocalServer {
        public int id;
        public int port;
    }

    private static class YamlRemoteServer {
        public String host;
        public int port;
        public int id;
    }
}
