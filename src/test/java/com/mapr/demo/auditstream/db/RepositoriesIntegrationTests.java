package com.mapr.demo.auditstream.db;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.demo.auditstream.db.repository.FileInfoRepository;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import com.mapr.demo.auditstream.utils.DataUtils;
import com.mapr.springframework.data.maprdb.core.MapROperations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(locations = "/application.properties")
@ContextConfiguration(classes = {DatabaseConfig.class})
public class RepositoriesIntegrationTests {

    @Autowired
    public FileInfoRepository fileInfoRepository;

    @Autowired
    public VolumeRepository volumeRepository;

    @Autowired
    public MapROperations operations;

    @Before
    public void init() {
        clean();
        operations.createTable(Volume.class);
        operations.createTable(FileInfo.class);
    }

    @After
    public void clean() {
        operations.dropTable(FileInfo.class);
        operations.dropTable(Volume.class);
    }

    @Test
    public void volumeSaveAndReadTest() {
        Volume volume = DataUtils.generateVolume(1);
        volumeRepository.save(volume);

        Optional<Volume> opVolume = volumeRepository.findById(volume.getId());

        Assert.assertTrue(opVolume.isPresent());
        Assert.assertEquals(volume, opVolume.get());
    }

    @Test
    public void fileSaveAndReadTest() {
        FileInfo file = DataUtils.generateFile(1);
        fileInfoRepository.save(file);

        Optional<FileInfo> oplFile = fileInfoRepository.findById(file.getFid());

        Assert.assertTrue(oplFile.isPresent());
        Assert.assertEquals(file, oplFile.get());
    }


}
