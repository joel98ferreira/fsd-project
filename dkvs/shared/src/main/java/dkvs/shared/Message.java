package dkvs.shared;

import io.atomix.utils.serializer.SerializerBuilder;

import java.io.Serializable;

public class Message implements Serializable {

    private final String id;
    private final RequestType type;
    // TODO: adicionar timestamp
    private final Object content;

    private SerializerBuilder serializerBuilder;

    public Message(final String id, final RequestType type, final Object content) {
        this.id = id;
        this.type = type;
        this.content = content;
    }

    public String getId() {
        return this.id;
    }

    public RequestType getType() {
        return type;
    }

    public Object getContent() {
        return content;
    }
}
