package com.mapr.demo.auditstream.fs;

import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(locations = "/application.properties")
@ContextConfiguration(classes = {MapRFsConfig.class})
public class MapRFsIntegrationTests {
    @Autowired
    public MapRFileSystem fs;

    @Value("${folder:/}")
    public String parentFolder;
    public Path testFolderPath;
    public String testFolderFid;

    @Before
    public void create() throws IOException {
        testFolderPath = new Path(formatPath(parentFolder, "testfolder"));
        testFolderFid = fs.mkdirsFid(testFolderPath);
    }

    @After
    public void delete() throws IOException {
        fs.removeRecursive(testFolderPath);
    }

    @Test
    public void isDirectoryTest() throws IOException {
        Assert.assertTrue(fs.isDirectory(testFolderPath));
    }

    @Test
    public void fidToPath() throws IOException {
        Path path = new Path(fs.getMountPathFid(testFolderFid));
        Assert.assertEquals(testFolderPath, path);
    }

    public String formatPath(String parent, String child) {
        return String.format(parent.endsWith("/") ? "%s%s" : "%s/%s", parent, child);
    }

}
