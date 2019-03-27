package com.mapr.demo.auditstream.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public AdminService adminService() {
        return new AdminService();
    }

    @Bean
    public KafkaClient kafkaClient() {
        return new KafkaClient();
    }

}
