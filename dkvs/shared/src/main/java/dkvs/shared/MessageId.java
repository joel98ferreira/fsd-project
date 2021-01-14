package dkvs.shared;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Class that uses a string uuid to be a unique representation of a Message.
 */
public class MessageId implements Comparable<MessageId>, Serializable {

    private final String messageUUID;

    public MessageId(){
        this.messageUUID = UUID.randomUUID().toString();
    }

    public String getMessageId() {
        return this.messageUUID;
    }

    @Override
    public boolean equals(Object obj) {
        return
                obj != null &&
                        this.getClass() == obj.getClass() &&
                        this.getMessageId().equals(((MessageId) obj).getMessageId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getMessageId());
    }

    @Override
    public int compareTo(MessageId other) {
        return this.getMessageId().compareTo(other.getMessageId());
    }

    @Override
    public String toString() {
        return messageUUID;
    }
}
