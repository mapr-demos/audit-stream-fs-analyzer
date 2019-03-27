package com.mapr.demo.auditstream.kafka.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaCldbMessage {
    private String resource;
    private CldbOperation operation;
    private String path;

    @SuppressWarnings("unchecked")
    @JsonProperty("properties")
    private void unpackPathFromProperties(Object properties) {
        if (properties != null) {
            try {
                List<Map<String, Object>> propertiesList = (List<Map<String, Object>>) properties;
                path = (String) propertiesList.get(0).get("newvalue");
            } catch (ClassCastException ignored) {
            }

        }
    }
}
