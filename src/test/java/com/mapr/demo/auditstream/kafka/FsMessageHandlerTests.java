package com.mapr.demo.auditstream.kafka;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.demo.auditstream.db.repository.FileInfoRepository;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import com.mapr.demo.auditstream.kafka.entity.FsOperation;
import com.mapr.demo.auditstream.kafka.entity.KafkaFsMessage;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;
import java.util.Optional;

public class FsMessageHandlerTests {
    public MapRFileSystem fs;
    public FileInfoRepository fileInfoRepository;
    public VolumeRepository volumeRepository;
    public FsMessageHandler handler;

    @Before
    public void init() {
        fs = Mockito.mock(MapRFileSystem.class);
        fileInfoRepository = Mockito.mock(FileInfoRepository.class);
        volumeRepository = Mockito.mock(VolumeRepository.class);
        handler = new FsMessageHandler(fs, fileInfoRepository, volumeRepository);
    }

    @Test
    public void createFileTest() throws IOException {
        String fid = "1";
        String path = "/volume/folder/file";
        String volumeId = "1";
        String volumePath = "/volume";
        Date date = new Date();

        Volume volume = new Volume();
        volume.setId(volumeId);
        volume.setPath(volumePath);

        KafkaFsMessage msg = new KafkaFsMessage();
        msg.setOperation(FsOperation.CREATE);
        msg.setVolumeId(volumeId);
        msg.setChildFid(fid);
        msg.setDate(date);

        FileInfo file = FileInfo.builder()
                .fid(fid)
                .name("file")
                .path("/folder")
                .volumeId(volumeId)
                .updateDate(date)
                .build();

        Mockito.when(fs.getMountPathFid(fid)).thenReturn(path);
        Mockito.when(fs.isDirectory(new Path(path))).thenReturn(false);
        Mockito.when(volumeRepository.findById(volumeId)).thenReturn(Optional.of(volume));

        handler.handle(msg);
        Mockito.verify(fileInfoRepository).save(file);
    }

    @Test
    public void renameFileTest() throws IOException {
        String oldParentFid = "1";
        String newParentFid = "2";
        String oldName = "test";
        String newName = "newtest";
        String oldPath = "/volume/folder1";
        String newPath = "/volume/folder2";
        String volumeId = "1";
        String volumePath = "/volume";
        Date date = new Date();

        Volume volume = new Volume();
        volume.setId(volumeId);
        volume.setPath(volumePath);

        FileInfo oldFile = FileInfo.builder()
                .fid("123")
                .name(oldName)
                .path("/folder1")
                .volumeId(volumeId)
                .updateDate(new Date())
                .build();

        FileInfo newFile = FileInfo.builder()
                .fid("123")
                .name(newName)
                .path("/folder2")
                .volumeId(volumeId)
                .updateDate(date)
                .build();

        KafkaFsMessage msg = new KafkaFsMessage();
        msg.setOperation(FsOperation.RENAME);
        msg.setSrcFid(oldParentFid);
        msg.setDstFid(newParentFid);
        msg.setSrcName(oldName);
        msg.setDstName(newName);
        msg.setDate(date);
        msg.setVolumeId(volumeId);

        Mockito.when(fs.getMountPathFid(oldParentFid)).thenReturn(oldPath);
        Mockito.when(fs.getMountPathFid(newParentFid)).thenReturn(newPath);
        Mockito.when(fs.isDirectory(new Path(String.format("%s/%s", newPath, newName)))).thenReturn(false);
        Mockito.when(volumeRepository.findById(volumeId)).thenReturn(Optional.of(volume));
        Mockito.when(fileInfoRepository.findOneByNameAndPath(oldName, "/folder1")).thenReturn(Optional.of(oldFile));

        handler.handle(msg);
        Mockito.verify(fileInfoRepository).save(newFile);
    }

    @Test
    public void deleteFileTest() {
        String fid = "1";

        KafkaFsMessage msg = new KafkaFsMessage();
        msg.setOperation(FsOperation.DELETE);
        msg.setChildFid(fid);

        handler.handle(msg);
        Mockito.verify(fileInfoRepository).deleteById(fid);
    }

    @Test
    public void updateFileTest() {
        String fid = "1";
        String volumeId = "1";
        Date updateDate = new Date();

        FileInfo file = FileInfo.builder()
                .fid(fid)
                .name("file")
                .path("/folder")
                .volumeId(volumeId)
                .updateDate(new Date())
                .build();

        KafkaFsMessage msg = new KafkaFsMessage();
        msg.setOperation(FsOperation.READ);
        msg.setVolumeId(volumeId);
        msg.setSrcFid(fid);
        msg.setDate(updateDate);

        Mockito.when(fileInfoRepository.findById(volumeId)).thenReturn(Optional.of(file));

        handler.handle(msg);
        file.setAccessDate(updateDate);
        Mockito.verify(fileInfoRepository).save(file);
    }

}
