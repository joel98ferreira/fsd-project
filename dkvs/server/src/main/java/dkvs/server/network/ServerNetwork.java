package dkvs.server.network;

import dkvs.server.RequestHandler;
import dkvs.server.ServerConfig;
import dkvs.server.identity.ClientId;
import dkvs.server.identity.ServerAddress;
import dkvs.server.identity.ServerId;
import dkvs.shared.Connection;
import dkvs.shared.Message;
import dkvs.shared.Network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ServerNetwork {

    private final ServerId localServerId;
    private final Map<ServerId, ServerAddress> remoteServerAddresses;
    private final Map<ServerId, Network> remoteServersNetwork;

    public ServerNetwork(ServerConfig serverConfig) {
        this.localServerId = Objects.requireNonNull(serverConfig.getLocalServerId());
        this.remoteServerAddresses = Objects.requireNonNull(serverConfig.getRemoteServers());
        this.remoteServersNetwork = new HashMap<>();
    }

    public void start(AsynchronousChannelGroup g, RequestHandler requestHandler) throws IOException {

        System.out.println("-----> When you are sure that ALL KNOWN SERVERS ARE RUNNING press any KEY!");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        in.readLine();

        for (Map.Entry<ServerId, ServerAddress> remoteServer : remoteServerAddresses.entrySet()) {
            Connection.connect(remoteServer.getValue().getSocketAddress(), g).thenAccept(network -> {

                System.out.println("> Connected successfully with the remote server: " + remoteServer.getValue().toString());

                // Register server payloads and start network
                registerServerPayloadsAndStartNetwork(network);

                // Insert in the map the network connecting this server with his known peers
                remoteServersNetwork.put(remoteServer.getKey(), Objects.requireNonNull(network));

                // Start receiving messages
                receive(remoteServer.getKey(), network, requestHandler);
            });
        }
    }

    /**
     * Method that register in a server network the payload type used to communicate between
     * server network and starts the network.
     */
    public static void registerServerPayloadsAndStartNetwork(Network network){
        network.registerPayloadType(ServerResponseMessageContent.class);
        network.registerPayloadType(ServerRequestMessageContent.class);
        network.registerPayloadType(AbstractMap.SimpleEntry.class);
        network.registerPayloadType(ScalarLogicalClock.class);
        network.registerPayloadType(ServerId.class);
        network.start();
    }

    /**
     * Method that returns a list of all of the remove servers.
     */
    public List<ServerId> getRemoteServersIds(){
        return new ArrayList<>(this.remoteServersNetwork.keySet());
    }

    /**
     * Method that given a known remote server sends a message to that server.
     * @param serverId The server id of the known server.
     * @param message The message to send to the known server.
     * @throws IOException
     */
    public void send(final ServerId serverId, final Message message) throws IOException {
        if (remoteServersNetwork.containsKey(serverId)){
            remoteServersNetwork.get(serverId).send(message).thenAccept(v -> {
                System.out.println("> Sent message " + message.getId() + " with type " + message.getType() +
                        " to: " + serverId.toString() + " with success!");
            });
        }
    }

    public CompletableFuture<Void> receive(final ServerId serverId, final Network network, final RequestHandler requestHandler){

        // Generate a new client Id for this server
        ClientId clientId = new ClientId(serverId.toString());

        CompletableFuture<Void> acceptor = new CompletableFuture<>();

        network.receive().thenAccept(m -> {
            if (m == null){
                System.out.println("> Connection closed, disconnecting!");
                remoteServersNetwork.get(serverId).close();
                remoteServersNetwork.remove(serverId);
                remoteServerAddresses.remove(serverId);
                return;
            }

            acceptor.complete(null);

            System.out.println("> Received message with id " + m.getId() + " and type " + m.getType() +
                    " with success from " + serverId.toString() + "!");

            try {
                requestHandler.handleMessage(m, clientId, remoteServersNetwork.get(serverId));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return acceptor;
    }

    public ServerId getLocalServerId() {
        return localServerId;
    }
}
