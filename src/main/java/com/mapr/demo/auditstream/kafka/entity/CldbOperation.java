package com.mapr.demo.auditstream.kafka.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CldbOperation {
    @JsonProperty("volumeMount")
    VOLUME_MOUNT,
    @JsonProperty("volumeUnmount")
    VOLUME_UNMOUNT,
    @JsonProperty("volumeRemove")
    VOLUME_REMOVE
}
