import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestDataTreatment {

    @Test
    public void getSerialize() throws IOException, ClassNotFoundException {
        Collection<Long> keys = new ArrayList<Long>();
        long l1 = 15000000L;
        long l2 = 1000000L;
        keys.add(l1);
        keys.add(l2);
        byte[] bytes = DataTreatment.serializeGet(keys);
        DataTreatment.MessageContent messageContent =
                DataTreatment.deserialize(bytes);

        Assert.assertEquals(DataTreatment.MessageType.GET_SEND, messageContent.getType());
        Collection<Long> newKeys = (Collection<Long>) messageContent.getContent();
        Assert.assertEquals(keys, newKeys);

    }

    @Test
    public void putSerialize() throws IOException, ClassNotFoundException {
        Map<Long, byte[]> values = new HashMap<Long, byte[]>();
        final long l1 = 15000000L;
        final String str1 = "ola";
        final String str2 = "adeus";
        final long l2 = 1000000L;
        byte[] arrayB1 = str1.getBytes(StandardCharsets.UTF_8);
        byte[] arrayB2 = str2.getBytes(StandardCharsets.UTF_8);
        values.put(l1, arrayB1);
        values.put(l2, arrayB2);
        byte[] bytes = DataTreatment.serializePut(values);
        DataTreatment.MessageContent messageContent =
                DataTreatment.deserialize(bytes);

        Assert.assertEquals(DataTreatment.MessageType.PUT_SEND, messageContent.getType());
        Map<Long, byte[]> newValues = (Map<Long, byte[]>) messageContent.getContent();
        String newStr1 = new String(newValues.get(l1), StandardCharsets.UTF_8);
        String newStr2 = new String(newValues.get(l2), StandardCharsets.UTF_8);

        Assert.assertEquals(str1,  newStr1);
        Assert.assertEquals(str2,  newStr2);
    }

    @Test
    public void serversReply() throws IOException, ClassNotFoundException {
        Map<Long, byte[]> values = new HashMap<Long, byte[]>();
        final long l1 = 15000000L;
        final String str1 = "ola";
        final String str2 = "adeus";
        final long l2 = 1000000L;
        byte[] arrayB1 = str1.getBytes(StandardCharsets.UTF_8);
        byte[] arrayB2 = str2.getBytes(StandardCharsets.UTF_8);
        values.put(l1, arrayB1);
        values.put(l2, arrayB2);

        //testa o reply do get
        byte[] bytes = DataTreatment.serializeReplyGet(values);
        DataTreatment.MessageContent messageContent =
                DataTreatment.deserialize(bytes);

        Assert.assertEquals(DataTreatment.MessageType.GET_REPLY, messageContent.getType());
        Map<Long, byte[]> newValues = (Map<Long, byte[]>) messageContent.getContent();
        String newStr1 = new String(newValues.get(l1), StandardCharsets.UTF_8);
        String newStr2 = new String(newValues.get(l2), StandardCharsets.UTF_8);

        Assert.assertEquals(str1, newStr1);
        Assert.assertEquals(str2, newStr2);

        // testa o reply do put
        final String s = "ok";
        byte[] replyOk = DataTreatment.serializeReplyPut(s);
        DataTreatment.MessageContent messageOK =
                DataTreatment.deserialize(replyOk);

        Assert.assertEquals(DataTreatment.MessageType.PUT_REPLY, messageOK.getType());
        Assert.assertEquals(s, messageOK.getContent());
    }
}
