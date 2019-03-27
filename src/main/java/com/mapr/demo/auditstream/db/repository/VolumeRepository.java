package com.mapr.demo.auditstream.db.repository;

import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.springframework.data.maprdb.repository.MapRRepository;

import java.util.Optional;

public interface VolumeRepository extends MapRRepository<Volume, String> {
    Optional<Volume> findOneByName(String name);
}
