package com.mapr.demo.auditstream.fs;

import com.mapr.fs.MapRFileSystem;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;

import java.net.URI;

@org.springframework.context.annotation.Configuration
public class MapRFsConfig {

    private static final String MAPRFS_URI = "maprfs:///";

    @Bean
    @SneakyThrows
    public MapRFileSystem getFS() {
        String uri = MAPRFS_URI;
        uri = uri + "mapr/";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", uri);

        MapRFileSystem fs = new MapRFileSystem();
        fs.initialize(URI.create(uri), conf, true);

        return fs;
    }

}
