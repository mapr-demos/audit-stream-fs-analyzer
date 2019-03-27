package com.mapr.demo.auditstream;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.springframework.data.maprdb.core.MapROperations;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationContextTests {

    @Autowired
    public MapROperations operations;

    @After
    public void clean() {
        operations.dropTable(Volume.class);
        operations.dropTable(FileInfo.class);
    }

    @Test
    public void contextLoads() {
    }

}

