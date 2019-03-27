package com.mapr.demo.auditstream.db;

import com.mapr.springframework.data.maprdb.config.AbstractMapRConfiguration;
import com.mapr.springframework.data.maprdb.repository.config.EnableMapRRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableMapRRepository
public class DatabaseConfig extends AbstractMapRConfiguration {

    @Value("${database.name:/}")
    private String databaseName;

    @Value("${database.host:host}")
    private String host;

    @Value("${database.username:mapr}")
    private String username;

    @Value("${database.password:mapr}")
    private String password;

    @Override
    protected String getDatabaseName() {
        return databaseName;
    }

    @Override
    protected String getHost() {
        return host;
    }

    @Override
    protected String getUsername() {
        return username;
    }

    @Override
    protected String getPassword() {
        return password;
    }

}
