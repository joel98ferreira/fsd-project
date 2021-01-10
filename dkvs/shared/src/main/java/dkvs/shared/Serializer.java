package dkvs.shared;

import java.io.*;

public class Serializer {

    public byte[] serialize(Message message) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeUTF(message.getId());
            out.write(message.getType().getType());
            out.writeObject(message.getContent());
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

    public Message deserialize(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            String id = in.readUTF();
            RequestType type = RequestType.getMessageType(in.read());
            Object content = in.readObject();
            return new Message(id, type, content);
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