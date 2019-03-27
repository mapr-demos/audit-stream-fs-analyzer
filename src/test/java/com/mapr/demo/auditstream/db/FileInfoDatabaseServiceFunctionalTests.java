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

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(locations = "/application.properties")
@ContextConfiguration(classes = {DatabaseConfig.class, FileInfoDatabaseService.class})
public class FileInfoDatabaseServiceFunctionalTests {
    public final static int LIST_SIZE = 10000;
    public final static int PAGE_SIZE = 100;
    public final static int VOLUME_AMOUNT = 2;

    public List<FileInfo> files;

    @Autowired
    public FileInfoDatabaseService service;

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

        List<Volume> volumes = volumeRepository.saveAll(DataUtils.generateVolumes(VOLUME_AMOUNT));
        List<FileInfo> tempFiles = fileInfoRepository.saveAll(DataUtils.generateFiles(LIST_SIZE, volumes));
        files = addPath(tempFiles, volumes);
    }

    @After
    public void clean() {
        operations.dropTable(FileInfo.class);
        operations.dropTable(Volume.class);
    }

    @Test
    public void getAllTest() {
        Assert.assertEquals(files, service.getAllRecords());
    }

    @Test
    public void getFirstPageTest() {
        List<FileInfo> fileList = files.subList(0, PAGE_SIZE);
        List<FileInfo> fileListFromDB = service.getRecordsPageByParameters(0, PAGE_SIZE, null,
                null, null, null, null, null, null).getContent();

        Assert.assertEquals(fileList.size(), fileListFromDB.size());
        Assert.assertEquals(fileList, fileListFromDB);
    }

    @Test
    public void getLastPageTest() {
        List<FileInfo> fileList = files.subList(LIST_SIZE - PAGE_SIZE, LIST_SIZE);
        List<FileInfo> fileListFromDB = service.getRecordsPageByParameters(LIST_SIZE / PAGE_SIZE, PAGE_SIZE,
                files.get(LIST_SIZE - PAGE_SIZE - 1).getFid(), null, null, null, null,
                null, null).getContent();

        Assert.assertEquals(fileList.size(), fileListFromDB.size());
        Assert.assertEquals(fileList, fileListFromDB);
    }

    @Test
    public void getNextPageTest() {
        List<FileInfo> fileList = files.subList(2 * PAGE_SIZE, 3 * PAGE_SIZE);
        List<FileInfo> fileListFromDB = service.getRecordsPageByParameters(2, PAGE_SIZE,
                files.get(2 * PAGE_SIZE - 1).getFid(), null, null, null, null, null,
                null).getContent();

        Assert.assertEquals(fileList.size(), fileListFromDB.size());
        Assert.assertEquals(fileList, fileListFromDB);
    }

    @Test
    public void getPreviousPageTest() {
        List<FileInfo> fileList = files.subList(2 * PAGE_SIZE, 3 * PAGE_SIZE);
        List<FileInfo> fileListFromDB = service.getRecordsPageByParameters(2, PAGE_SIZE,
                files.get(3 * PAGE_SIZE).getFid(), true, null, null, null, null,
                null).getContent();

        Assert.assertEquals(fileList.size(), fileListFromDB.size());
        Assert.assertEquals(fileList, fileListFromDB);
    }

    @Test
    public void getPageFilteredByUpdateDate() {
        Date date = files.get(PAGE_SIZE / 2).getUpdateDate();
        List<FileInfo> fileList = files.stream().filter(f -> f.getUpdateDate().getTime() < date.getTime())
                .limit(PAGE_SIZE).collect(Collectors.toList());
        List<FileInfo> fileListFromDB = service.getRecordsPageByParameters(0, PAGE_SIZE, null,
                null, null, null, null, null, date.getTime()).getContent();

        Assert.assertEquals(fileList.size(), fileListFromDB.size());
        Assert.assertEquals(fileList, fileListFromDB);
    }

    @Test
    public void getPageFilteredByName() {
        String name = files.get(PAGE_SIZE / 2).getName();
        List<FileInfo> fileList = files.stream().filter(f -> f.getName().contains(name)).collect(Collectors.toList());
        List<FileInfo> fileListFromDB = service.getRecordsPageByParameters(0, PAGE_SIZE, null,
                null, null, name, null, null, null).getContent();

        Assert.assertEquals(fileList.size(), fileListFromDB.size());
        Assert.assertEquals(fileList, fileListFromDB);
    }

    public List<FileInfo> addPath(List<FileInfo> files, List<Volume> volumes) {
        Map<String, String> pathMap = volumes.stream().collect(Collectors.toMap(Volume::getId, Volume::getPath));
        return files.stream().peek(f -> f.setPath(pathMap.get(f.getVolumeId()) + f.getPath()))
                .sorted(Comparator.comparing(FileInfo::getFid)).collect(Collectors.toList());
    }

}
