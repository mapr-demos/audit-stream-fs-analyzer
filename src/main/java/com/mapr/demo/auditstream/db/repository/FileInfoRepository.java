package com.mapr.demo.auditstream.db.repository;

import com.mapr.springframework.data.maprdb.repository.MapRRepository;
import com.mapr.demo.auditstream.db.entity.FileInfo;

import java.util.Optional;

public interface FileInfoRepository extends MapRRepository<FileInfo, String> {
    Optional<FileInfo> findOneByNameAndPath(String name, String path);
    void deleteByVolumeId(String volumeId);
}
