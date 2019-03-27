package com.mapr.demo.auditstream.kafka;

import com.mapr.streams.Admin;
import com.mapr.streams.Streams;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public class AdminService {
    private Admin admin;

    @SneakyThrows
    public AdminService() {
        admin = Streams.newAdmin(new Configuration());
    }

    @SneakyThrows
    public Set<String> getTopicNames(final String stream) {
        return new HashSet<>(admin.listTopics(stream));
    }

}
