package dkvs.server;

import spullara.nio.channels.FutureSocketChannel;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Clients {
    Lock clients = new ReentrantLock();
    PriorityQueue<Client> idleClients =
            new PriorityQueue<Client>(1,
                    (x,y) -> {return Integer.compare(x.messageId, y.messageId);});
    Set<FutureSocketChannel> removedClients = new HashSet<>();
    int connected = 0;

    public void add(Client client){
        clients.lock();
        try {
            idleClients.add(client);
        } finally {
            connected++;
            clients.unlock();
        }
    }

    public void remove(FutureSocketChannel client){
        clients.lock();
        try {
            removedClients.add(client);
        } finally {
            connected--;
            clients.unlock();
        }
    }

    public Client handle(int lastMessageId){
        Client client = null;
        clients.lock();
        try {
            if(idleClients.size() > 0 && idleClients.peek().messageId < lastMessageId) {
                client = idleClients.remove();
                if(removedClients.contains(client.socket)){
                    removedClients.remove(client.socket);
                    client = null;
                }
            }
        } finally {
            clients.unlock();
        }
        return client;
    }

    public void handled(Client client){
        clients.lock();
        try {
            idleClients.add(client);
        } finally {
            clients.unlock();
        }
    }

    public int connected(){
        clients.lock();
        try {
            return connected;
        } finally {
            clients.unlock();
        }
    }

}
