package dkvs.server;

public class State {

    public final Messages messages;
    public final Clients clients;

    public State(){
        this.messages = new Messages();
        this.clients = new Clients();
    }

}
