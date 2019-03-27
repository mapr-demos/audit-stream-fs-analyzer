package com.mapr.demo.auditstream.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.demo.auditstream.kafka.entity.KafkaCldbMessage;
import com.mapr.demo.auditstream.kafka.entity.KafkaFsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class AuditStreamEventsHandler {
    private final static String STREAM = "/var/mapr/auditstream/auditlogstream";

    private final ObjectMapper mapper = new ObjectMapper();
    private final AdminService adminService;
    private final KafkaClient kafkaClient;
    private final CldbMessageHandler cldbMessageHandler;
    private final FsMessageHandler fsMessageHandler;

    @PostConstruct
    public void init() {
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        Set<String> allTopics = adminService.getTopicNames(STREAM);
        Set<String> topics = allTopics.stream().filter(topic -> topic.contains("_fs_")).map(this::convertToKafkaTopic)
                .collect(Collectors.toSet());
        log.info("Subscribe to FS events {}", topics);
        kafkaClient.subscribe(topics).subscribe(this::processFsData);
        topics = allTopics.stream().filter(topic -> topic.contains("_cldb_")).map(this::convertToKafkaTopic)
                .collect(Collectors.toSet());
        log.info("Subscribe to CLDB events {}", topics);
        kafkaClient.subscribe(topics).subscribe(this::processCldbData);
    }

    private void processFsData(ConsumerRecord<String, byte[]> record) {
        log.debug(new String(record.value()));
        try {
            KafkaFsMessage msg = mapper.readValue(record.value(), KafkaFsMessage.class);
            if (msg.getOperation() != null) {
                fsMessageHandler.handle(msg);
                log.debug(msg.toString());
            }
        } catch (IOException e) {
            log.error("Failed to parse {}", new String(record.value()), e);
        }
    }

    private void processCldbData(ConsumerRecord<String, byte[]> record) {
        log.debug(new String(record.value()));
        try {
            KafkaCldbMessage msg = mapper.readValue(record.value(), KafkaCldbMessage.class);
            if (msg.getOperation() != null) {
                cldbMessageHandler.handle(msg);
                log.debug(msg.toString());
            }
        } catch (IOException e) {
            log.error("Failed to parse {}", new String(record.value()), e);
        }
    }

    private String convertToKafkaTopic(String topic) {
        return String.format("%s:%s", STREAM, topic);
    }

}
