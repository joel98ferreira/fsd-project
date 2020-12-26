package dkvs.server;

import spullara.nio.channels.FutureSocketChannel;

public class Client {
    public final FutureSocketChannel socket;
    public int messageId;

    public Client(FutureSocketChannel socket, int messageId){
        this.socket = socket;
        this.messageId = messageId;
    }
}
