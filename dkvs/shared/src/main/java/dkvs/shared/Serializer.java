package dkvs.shared;

import java.io.*;
import java.util.Collection;
import java.util.Map;

public class Serializer {

    public byte[] serializePut(Map<Long, byte[]> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(RequestType.PUT_SEND.getType());
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

    public byte[] serializeGet(Collection<Long> keys) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(RequestType.GET_SEND.getType());
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
    public byte[] serializeReplyGet(Map<Long, byte[]> values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(RequestType.GET_REPLY.getType());
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
    public byte[] serializeReplyPut(String ok) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.write(RequestType.PUT_REPLY.getType());
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

    public Message deserialize(byte[] value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            RequestType type = RequestType.getMessageType(in.read());
            Object content = in.readObject();
            return new Message(type, content);
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
