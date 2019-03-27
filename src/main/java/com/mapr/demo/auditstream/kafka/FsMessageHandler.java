package com.mapr.demo.auditstream.kafka;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.demo.auditstream.db.repository.FileInfoRepository;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import com.mapr.demo.auditstream.kafka.entity.FsOperation;
import com.mapr.demo.auditstream.kafka.entity.KafkaFsMessage;
import com.mapr.fs.MapRFileSystem;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class FsMessageHandler {
    private final MapRFileSystem fs;
    private final FileInfoRepository fileInfoRepository;
    private final VolumeRepository volumeRepository;

    public void handle(KafkaFsMessage msg) {
        if (msg.getOperation() != null) {
            switch (msg.getOperation()) {
                case CREATE:
                    create(msg);
                    break;
                case RENAME:
                    rename(msg);
                    break;
                case DELETE:
                    delete(msg);
                    break;
                default:
                    update(msg);
            }
        }
    }

    @SneakyThrows
    private void rename(KafkaFsMessage msg) {
        log.debug("Rename event");
        String stringPath = convertToPathFormat(fs.getMountPathFid(msg.getDstFid()), msg.getDstName());
        Path newPath = new Path(stringPath);

        if (!fs.isDirectory(newPath)) {
            String stringOldPath = convertToPathFormat(fs.getMountPathFid(msg.getSrcFid()), msg.getSrcName());
            Path oldPath = new Path(stringOldPath);
            Optional<Volume> opVolume = volumeRepository.findById(msg.getVolumeId());

            if (opVolume.isPresent()) {
                Optional<FileInfo> opFileInfo = fileInfoRepository.findOneByNameAndPath(oldPath.getName(),
                        oldPath.getParent().toString().replace(opVolume.get().getPath(), ""));

                if (opFileInfo.isPresent()) {
                    FileInfo fileInfo = opFileInfo.get();
                    log.info("Renaming file from {} to {} fid:{} volume{}", oldPath, newPath, fileInfo.getFid(),
                            fileInfo.getVolumeId());
                    fileInfo.setName(newPath.getName());
                    fileInfo.setPath(removeVolumePath(newPath.getParent(), msg.getVolumeId()));
                    fileInfo.setUpdateDate(msg.getDate());
                    fileInfoRepository.save(fileInfo);
                }
            }
        }
    }

    @SneakyThrows
    private void create(KafkaFsMessage msg) {
        log.debug("Create event");
        if (msg.getChildFid() != null) {
            String stringPath = fs.getMountPathFid(msg.getChildFid());
            Path path = stringPath != null ? new Path(stringPath) : null;

            if (path != null && !fs.isDirectory(path)) {
                FileInfo fileInfo = FileInfo.builder()
                        .fid(msg.getChildFid())
                        .name(path.getName())
                        .path(removeVolumePath(path.getParent(), msg.getVolumeId()))
                        .volumeId(msg.getVolumeId())
                        .build();
                log.info("Creating file {} fid:{} volume:{}", fileInfo.getPath(), fileInfo.getFid(),
                        fileInfo.getVolumeId());
                fileInfo.setUpdateDate(msg.getDate());
                fileInfoRepository.save(fileInfo);
            }
        }
    }

    private void delete(KafkaFsMessage msg) {
        log.debug("Delete event");
        log.info("Deleting file fid:{} volume:{}", msg.getChildFid(), msg.getVolumeId());

        fileInfoRepository.deleteById(msg.getChildFid());
    }

    private void update(KafkaFsMessage msg) {
        log.debug("Update event");

        if (msg.getSrcFid() != null) {
            Optional<FileInfo> opFileInfo = fileInfoRepository.findById(msg.getSrcFid());
            if (opFileInfo.isPresent()) {
                FileInfo fileInfo = opFileInfo.get();
                log.info("Updating file {}/{} fid:{} volume:{}", fileInfo.getPath(), fileInfo.getName(),
                        fileInfo.getFid(), fileInfo.getVolumeId());

                if (msg.getOperation() == FsOperation.READ)
                    fileInfo.setAccessDate(msg.getDate());
                else
                    fileInfo.setUpdateDate(msg.getDate());

                fileInfoRepository.save(fileInfo);
            }
        }
    }

    private String convertToPathFormat(String folder, String name) {
        return String.format(folder.endsWith("/") ? "%s%s" : "%s/%s", folder, name);
    }

    private String removeVolumePath(Path filePath, String volumeId) {
        return volumeRepository.findById(volumeId)
                .map(volume -> filePath.toString().replace(volume.getPath(), ""))
                .orElseGet(filePath::toString);
    }

}
