package com.mapr.demo.auditstream.kafka;

import com.mapr.demo.auditstream.fs.MapRFsConfig;
import com.mapr.demo.auditstream.kafka.entity.KafkaFsMessage;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(locations = "/application.properties")
@ContextConfiguration(classes = {KafkaConfig.class, MapRFsConfig.class})
public class AuditStreamEventsHandlerIntegrationTests {
    public AuditStreamEventsHandler eventsHandler;
    public FsMessageHandler fsMessageHandler;

    @Autowired
    public AdminService admin;

    @Autowired
    public KafkaClient client;

    @Autowired
    public MapRFileSystem fs;

    @Value("${folder:/}")
    public String folder;

    public Path folderPath;

    @Before
    public void init() {
        fsMessageHandler = Mockito.mock(FsMessageHandler.class);
        eventsHandler = new AuditStreamEventsHandler(admin, client, null, fsMessageHandler);
        eventsHandler.init();
        folderPath = new Path(formatPath(folder, "testfolder"));
    }

    @After
    public void remove() throws IOException {
        fs.removeRecursive(folderPath);
    }

    @Test
    public void processFsEvent() throws InterruptedException, IOException {
        fs.mkdirs(folderPath);
        fs.modifyAudit(folderPath, true);
        Thread.sleep(10000);
        Mockito.verify(fsMessageHandler, atLeast(1)).handle(any(KafkaFsMessage.class));
    }

    public String formatPath(String parent, String child) {
        return String.format(parent.endsWith("/") ? "%s%s" : "%s/%s", parent, child);
    }

}
