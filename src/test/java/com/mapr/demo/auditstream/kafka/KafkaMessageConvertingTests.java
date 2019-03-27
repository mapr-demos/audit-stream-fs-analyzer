package com.mapr.demo.auditstream.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.demo.auditstream.kafka.entity.CldbOperation;
import com.mapr.demo.auditstream.kafka.entity.FsOperation;
import com.mapr.demo.auditstream.kafka.entity.KafkaCldbMessage;
import com.mapr.demo.auditstream.kafka.entity.KafkaFsMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

public class KafkaMessageConvertingTests {
    private final static ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void convertToFsFromJsonTest() throws IOException {
        String json = "{\n" +
                "  \"timestamp\": {\n" +
                "    \"$date\": \"2017-04-27T10:53:37.239Z\"\n" +
                "  },\n" +
                "  \"operation\": \"CREATE\",\n" +
                "  \"uid\": 0,\n" +
                "  \"ipAddress\": \"10.20.30.140\",\n" +
                "  \"nfsServer\": \"10.20.30.140\",\n" +
                "  \"parentFid\": \"2066.32.131358\",\n" +
                "  \"childFid\": \"2066.33.262630\",\n" +
                "  \"childName\": \"abc.txt\",\n" +
                "  \"srcFid\": \"123.45.6\",\n" +
                "  \"dstFid\": \"789.10.11\",\n" +
                "  \"srcName\": \"file\",\n" +
                "  \"dstName\": \"file2\",\n" +
                "  \"volumeId\": 106738640,\n" +
                "  \"status\": 0\n" +
                "}";

        KafkaFsMessage msg = new KafkaFsMessage();
        msg.setDate(new Date(1493290417239L));
        msg.setParentFid("2066.32.131358");
        msg.setChildFid("2066.33.262630");
        msg.setSrcFid("123.45.6");
        msg.setDstFid("789.10.11");
        msg.setSrcName("file");
        msg.setDstName("file2");
        msg.setOperation(FsOperation.CREATE);
        msg.setVolumeId("106738640");

        KafkaFsMessage msgFromJson = MAPPER.readValue(json, KafkaFsMessage.class);
        Assert.assertEquals(msg, msgFromJson);

        msg.setVolumeId("0");
        Assert.assertNotEquals(msg, msgFromJson);
    }

    @Test
    public void convertToCldbFromJsonTest() throws IOException {
        String json = "{\n" +
                "  \"timestamp\": {\n" +
                "    \"$date\": \"2019-02-18T11:37:13.382Z\"\n" +
                "  },\n" +
                "  \"resource\": \"test5\",\n" +
                "  \"operation\": \"volumeMount\",\n" +
                "  \"uid\": 5000,\n" +
                "  \"clientip\": \"10.0.11.15\",\n" +
                "  \"properties\": [\n" +
                "    {\n" +
                "      \"property\": \"mountDir\",\n" +
                "      \"oldvalue\": \"\",\n" +
                "      \"newvalue\": \"/test5\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"status\": 0\n" +
                "}";

        KafkaCldbMessage msg = new KafkaCldbMessage();
        msg.setResource("test5");
        msg.setOperation(CldbOperation.VOLUME_MOUNT);
        msg.setPath("/test5");

        KafkaCldbMessage msgFromJson = MAPPER.readValue(json, KafkaCldbMessage.class);
        Assert.assertEquals(msg, msgFromJson);

        msg.setOperation(CldbOperation.VOLUME_REMOVE);
        Assert.assertNotEquals(msg, msgFromJson);
    }

}
