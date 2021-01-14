package dkvs.server.network;

import dkvs.server.RequestHandler;
import dkvs.server.ServerConfig;
import dkvs.server.identity.ServerAddress;
import dkvs.server.identity.ServerId;
import dkvs.shared.Connection;
import dkvs.shared.Message;
import dkvs.shared.Network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ServerNetwork {

    private final Map<ServerId, ServerAddress> remoteServerAddresses;
    private final Map<ServerId, Network> remoteServersNetwork;
    private final Map<ServerId, ScalarLogicalClock> remoteProcessesClocks;

    public ServerNetwork(ServerConfig serverConfig) {
        this.remoteServerAddresses = Objects.requireNonNull(serverConfig.getRemoteServers());
        this.remoteServersNetwork = new HashMap<>();
        this.remoteProcessesClocks = new HashMap<>();
    }

    public void start() throws IOException {

        System.out.println("-----> When you are sure that ALL KNOWN SERVERS ARE RUNNING press any KEY!");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        in.readLine();

        for (Map.Entry<ServerId, ServerAddress> remoteServer : remoteServerAddresses.entrySet()) {
            Connection.connect(remoteServer.getValue().getSocketAddress()).thenAccept(network -> {

                System.out.println("> Connected successfully with the remote server: " + remoteServer.getValue().toString());

                // Register server payloads and start network
                registerServerPayloadsAndStartNetwork(network);

                // Insert in the map the network connecting this server with his known peers
                remoteServersNetwork.put(remoteServer.getKey(), Objects.requireNonNull(network));

                // Insert in the map with the scalar logical clocks a new clock
                remoteProcessesClocks.put(remoteServer.getKey(), new ScalarLogicalClock());

               // receive(remoteServer.getKey(), network, requestHandler);
            });
        }
    }

    /**
     * Method that register in a server network the payload type used to communicate between
     * server network and starts the network.
     */
    public static void registerServerPayloadsAndStartNetwork(Network network){
        network.registerPayloadType(ServerResponseContent.class);
        network.registerPayloadType(ServerId.class);
        network.start();
    }

    /**
     * Method that given a known remote server sends a message to that server.
     * @param serverId The server id of the known server.
     * @param message The message to send to the known server.
     * @throws IOException
     */
    public void send(final ServerId serverId, final Message message, final RequestHandler requestHandler) throws IOException {
        if (remoteServersNetwork.containsKey(serverId)){
            remoteServersNetwork.get(serverId).send(message).thenAccept(v -> {
                System.out.println("> Sent message " + message.getId() + " with type " + message.getType() +
                        " to: " + serverId.toString() + " with success!");

                receive(serverId, remoteServersNetwork.get(serverId), requestHandler);
            });
        }
    }

    public void receive(final ServerId serverId, final Network network, final RequestHandler requestHandler){
        network.receive().thenAccept(m -> {
            if (m == null){
                System.out.println("> Connection closed, disconnecting!");
                remoteServersNetwork.get(serverId).close();
                remoteServersNetwork.remove(serverId);
                remoteProcessesClocks.remove(serverId);
                remoteServerAddresses.remove(serverId);
                return;
            }

            System.out.println("> Received message with id " + m.getId() + " and type " + m.getType() +
                    " with success from " + serverId.toString() + "!");

            requestHandler.handleMessage(m,  null, remoteServersNetwork.get(serverId));

            receive(serverId, network, requestHandler);
        });
    }
}
