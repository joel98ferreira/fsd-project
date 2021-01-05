package dkvs.shared;

import io.atomix.utils.serializer.SerializerBuilder;

import java.io.Serializable;

public class Message implements Serializable {
    private final RequestType type;
    // TODO: adicionar timestamp
    private final Object content;

    private SerializerBuilder serializerBuilder;

    public Message(final RequestType type, final Object content) {
        this.type = type;
        this.content = content;
    }

    public RequestType getType() {
        return type;
    }

    public Object getContent() {
        return content;
    }
}
