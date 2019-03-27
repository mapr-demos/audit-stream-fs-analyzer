package com.mapr.demo.auditstream.kafka;

import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.demo.auditstream.db.repository.FileInfoRepository;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import com.mapr.demo.auditstream.kafka.entity.CldbOperation;
import com.mapr.demo.auditstream.kafka.entity.KafkaCldbMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class CldbMessageHandlerTests {
    public FileInfoRepository fileInfoRepository;
    public VolumeRepository volumeRepository;
    public CldbMessageHandler handler;

    @Before
    public void init() {
        fileInfoRepository = Mockito.mock(FileInfoRepository.class);
        volumeRepository = Mockito.mock(VolumeRepository.class);
        handler = new CldbMessageHandler(volumeRepository, fileInfoRepository);
    }

    @Test
    public void updateVolume() {
        String volumeId = "1";
        String name = "volume";
        String oldMountPath = "/volume1";
        String newMountPath = "/volume2";

        Volume oldVolume = new Volume();
        oldVolume.setId(volumeId);
        oldVolume.setPath(oldMountPath);
        oldVolume.setName(name);

        Volume newVolume = new Volume();
        newVolume.setId(volumeId);
        newVolume.setPath(newMountPath);
        newVolume.setName(name);

        KafkaCldbMessage msg = new KafkaCldbMessage();
        msg.setOperation(CldbOperation.VOLUME_MOUNT);
        msg.setPath(newMountPath);
        msg.setResource(name);

        Mockito.when(volumeRepository.findOneByName(name)).thenReturn(Optional.of(oldVolume));

        handler.handle(msg);
        Mockito.verify(volumeRepository).save(newVolume);
    }

    @Test
    public void removeVolumeTest() {
        String volumeId = "1";
        String name = "volume";
        String path = "/volume";

        Volume volume = new Volume();
        volume.setId(volumeId);
        volume.setPath(path);
        volume.setName(name);

        KafkaCldbMessage msg = new KafkaCldbMessage();
        msg.setOperation(CldbOperation.VOLUME_REMOVE);
        msg.setResource(name);

        Mockito.when(volumeRepository.findOneByName(name)).thenReturn(Optional.of(volume));

        handler.handle(msg);
        Mockito.verify(fileInfoRepository).deleteByVolumeId(volumeId);
        Mockito.verify(volumeRepository).deleteById(volumeId);
    }

}
