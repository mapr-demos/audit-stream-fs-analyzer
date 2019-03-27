package com.mapr.demo.auditstream.kafka;

import com.mapr.demo.auditstream.cli.VolumeCli;
import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.demo.auditstream.db.repository.FileInfoRepository;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import com.mapr.demo.auditstream.kafka.entity.CldbOperation;
import com.mapr.demo.auditstream.kafka.entity.KafkaCldbMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CldbMessageHandler {
    private final VolumeRepository volumeRepository;
    private final FileInfoRepository fileRepository;

    public void handle(KafkaCldbMessage msg) {
        if (msg.getOperation() != null && msg.getResource() != null) {
            if (msg.getOperation() == CldbOperation.VOLUME_REMOVE)
                removeVolumeAndFiles(msg.getResource());
            else
                updateVolume(msg);
        }
    }

    private void updateVolume(KafkaCldbMessage msg) {
        log.info("Update event");
        Optional<Volume> opVolume = volumeRepository.findOneByName(msg.getResource());
        if (opVolume.isPresent()) {
            log.info("Updating volume {}", msg.getResource());
            Volume volume = opVolume.get();
            volume.setPath(msg.getPath());
            volumeRepository.save(volume);
        } else {
            log.info("Creating new volume {}", msg.getResource());
            VolumeCli.getVolumeByName(msg.getResource()).ifPresent(volumeRepository::save);
        }
    }

    private void removeVolumeAndFiles(String name) {
        log.info("Remove event");
        Optional<Volume> opVolume = volumeRepository.findOneByName(name);
        if (opVolume.isPresent()) {
            log.info("Deleting volume {}", name);
            fileRepository.deleteByVolumeId(opVolume.get().getId());
            volumeRepository.deleteById(opVolume.get().getId());
        }
    }
}
