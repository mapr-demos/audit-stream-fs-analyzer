package com.mapr.demo.auditstream.kafka.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.SneakyThrows;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaFsMessage {
    @JsonIgnore
    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private Date date;
    private String parentFid;
    private String childFid;
    private String srcFid;
    private String dstFid;
    private String srcName;
    private String dstName;
    private FsOperation operation;
    private String volumeId;

    @SneakyThrows
    @JsonProperty("timestamp")
    private void unpackNameFromNestedObject(Map<String, String> timestamp) {
        date = SDF.parse(timestamp.get("$date").replace("Z", "+0000"));
    }

}
