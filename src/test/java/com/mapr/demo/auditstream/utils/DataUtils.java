package com.mapr.demo.auditstream.utils;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.db.entity.Volume;
import lombok.experimental.UtilityClass;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@UtilityClass
public class DataUtils {

    public List<Volume> generateVolumes(int size) {
        return IntStream.range(1, size + 1).mapToObj(DataUtils::generateVolume).collect(Collectors.toList());
    }

    public List<FileInfo> genetateFiles(int size) {
        return IntStream.range(1, size + 1).mapToObj(DataUtils::generateFile).collect(Collectors.toList());
    }

    public List<FileInfo> generateFiles(int size, List<Volume> volumes) {
        return IntStream.range(1, size + 1).mapToObj(i -> generateFile(i, volumes.get(i % volumes.size()).getId()))
                .collect(Collectors.toList());
    }

    public Volume generateVolume(Integer id) {
        Volume volume = new Volume();

        volume.setId(id.toString());
        volume.setName("test" + id);
        volume.setPath("/volume" + id);

        return volume;
    }

    public FileInfo generateFile(Integer id) {
        return generateFile(id, "1");
    }

    public FileInfo generateFile(Integer id, String volumeId) {
        return FileInfo.builder()
                .fid(id.toString())
                .path("/folder")
                .name("testfile" + id)
                .volumeId(volumeId)
                .updateDate(new Date())
                .accessDate(new Date())
                .build();
    }

}
