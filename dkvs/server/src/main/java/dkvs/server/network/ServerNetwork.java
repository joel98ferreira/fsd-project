package dkvs.server.network;

import dkvs.server.ServerConfig;
import dkvs.server.identity.ServerAddress;
import dkvs.server.identity.ServerId;
import dkvs.shared.Connection;
import dkvs.shared.Network;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ServerNetwork {

    private final ServerId localServerId;
    private final Map<ServerId, ServerAddress> remoteServerAddresses;
    private final Map<ServerId, Network> remoteServersNetwork;

    public ServerNetwork(ServerConfig serverConfig) {
        this.localServerId = Objects.requireNonNull(serverConfig.getLocalServerId());
        this.remoteServerAddresses = Objects.requireNonNull(serverConfig.getRemoteServers());
        this.remoteServersNetwork = new HashMap<>();
    }

    public void start() throws IOException {

        for (Map.Entry<ServerId, ServerAddress> remoteServer : remoteServerAddresses.entrySet()) {
            Connection.connect(remoteServer.getValue().getSocketAddress()).thenAccept(network -> {

                System.out.println("> Connected successfully with the remote server: " + remoteServer.getValue().toString());

                // Insert in the map the network connecting this server with his known peers
                remoteServersNetwork.put(remoteServer.getKey(), network);
            });
        }
    }
}
