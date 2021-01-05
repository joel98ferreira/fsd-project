package dkvs.server.identity;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ServerAddress {
    private final InetSocketAddress address;

    public ServerAddress(String host, int port) {
        this.address = new InetSocketAddress(host, port);
    }

    public SocketAddress getSocketAddress() {
        return address;
    }

    public int getPort(){
        return address.getPort();
    }

    public String getHost(){
        return address.getHostName();
    }

    @Override
    public String toString() {
        return address.getHostName() + ":" + address.getPort();
    }
}
