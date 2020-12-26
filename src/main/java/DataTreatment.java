import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DataTreatment {
    public enum MessageType {
        PUT_SEND(1),
        PUT_REPLY(2),
        GET_SEND(3),
        GET_REPLY(4);

        private final int type;

        MessageType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        private final static Map<Integer, MessageType> map =
                new HashMap<Integer, MessageType>();

        static {
            for (MessageType type : MessageType.values()) {
                map.put(type.getType(), type);
            }
        }

        static MessageType getMessageType(int type) {
            return map.get(type);
        }
    }

    public static class MessageContent {
        final private MessageType type;
        final private Object content;

        public MessageContent(final MessageType type, final Object content) {
            this.type = type;
            this.content = content;
        }

        public MessageType getType() {
            return type;
        }

        public Object getContent() {
            return content;
        }
    }

    public static byte[] serializePut(Map<Long, byte[]> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(MessageType.PUT_SEND.getType());
            out.writeObject(values);
            out.flush();
            bos.close();
            return bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                ex.getStackTrace();
            }
        }
    }

    public static byte[] serializeGet(Collection<Long> keys) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(MessageType.GET_SEND.getType());
            out.writeObject(keys);
            out.flush();
            return bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                ex.getStackTrace();
            }
        }
    }

    //Serializar a resposta do servidor ao pedido do cliente de um Get
    public static byte[] serializeReplyGet(Map<Long, byte[]> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(MessageType.GET_REPLY.getType());
            out.writeObject(values);
            out.flush();
            return bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                ex.getStackTrace();
            }
        }
    }

    //Serializar a resposta do servidor ao pedido do cliente de um Put. A resposta do servidor é um "ok"
    public static byte[] serializeReplyPut(String ok) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(MessageType.PUT_REPLY.getType());
            out.writeObject(ok);
            out.flush();
            return bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                ex.getStackTrace();
            }
        }
    }

    public static MessageContent deserialize(byte[] value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            MessageType type = MessageType.getMessageType(in.read());
            Object content = in.readObject();
            return new MessageContent(type, content);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.getStackTrace();
            }
        }
    }
}