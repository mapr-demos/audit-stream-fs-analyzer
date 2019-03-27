package com.mapr.demo.auditstream.db;

import com.mapr.demo.auditstream.cli.VolumeCli;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@RequiredArgsConstructor
public class VolumeDatabaseInitializer {

    private final VolumeRepository repository;

    @PostConstruct
    public void init() {
        repository.saveAll(VolumeCli.getAllVolumes());
    }

}
