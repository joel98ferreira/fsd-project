package dkvs.shared;

import io.atomix.utils.serializer.SerializerBuilder;

import java.io.Serializable;

public class Message implements Serializable {

    private final MessageId id;
    private final RequestType type;
    // TODO: adicionar timestamp
    private final Object content;

    private SerializerBuilder serializerBuilder;

    public Message(final MessageId id, final RequestType type, final Object content) {
        this.id = id;
        this.type = type;
        this.content = content;
    }

    public MessageId getId() {
        return this.id;
    }

    public RequestType getType() {
        return type;
    }

    public Object getContent() {
        return content;
    }
}
