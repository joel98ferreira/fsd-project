package dkvs.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Messages {
    private final Lock writes = new ReentrantLock();
    private final Condition newMessages = writes.newCondition();
    private final HashMap<Integer, String> messages = new HashMap<>();
    private int nextId = 0;

    public int currentId(){
        writes.lock();
        try {
            return nextId;
        } finally {
            writes.unlock();
        }
    }

    public void addMessage(String message){
        writes.lock();
        try {
            messages.put(nextId++, message);
            newMessages.signalAll();
        } finally {
            writes.unlock();
        }
    }

    public String getNextMessage(int lastMessageId) {
        writes.lock();
        try {
            if (lastMessageId >= nextId)
                return null;
            return messages.get(lastMessageId);
        } finally {
            writes.unlock();
        }
    }
}
