package com.mapr.demo.auditstream.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.demo.auditstream.db.entity.Volume;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class VolumeCliTest {

    @Test
    public void runCommandTest() {
        Assert.assertFalse(VolumeCli.run("ls").isEmpty());
    }

    @Test(expected = IOException.class)
    public void runInvalidCommandTest() {
        VolumeCli.run("INVALID_COMMAND");
    }

    @Test
    public void convertToVolumeTest() throws IOException {
        String line = "{\n" +
                "      \"mountdir\": \"\",\n" +
                "      \"volumename\": \"mapr.cldb.internal\",\n" +
                "      \"volumeid\": 1\n" +
                "    }";

        Volume volume = new Volume();
        volume.setId("1");
        volume.setName("mapr.cldb.internal");
        volume.setPath("");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(line);
        Volume resultVolume = VolumeCli.convertToVolume(node);

        Assert.assertEquals(volume, resultVolume);
    }

    @Test
    public void parseTest() {
        String line = "{\n" +
                "  \"timestamp\": 1551264393597,\n" +
                "  \"timeofday\": \"2019-02-27 10:46:33.597 GMT+0000 AM\",\n" +
                "  \"status\": \"OK\",\n" +
                "  \"total\": 1,\n" +
                "  \"data\": [\n" +
                "    {\n" +
                "      \"mountdir\": \"\",\n" +
                "      \"volumename\": \"mapr.cldb.internal\",\n" +
                "      \"volumeid\": 1\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Volume volume = new Volume();
        volume.setId("1");
        volume.setName("mapr.cldb.internal");
        volume.setPath("");

        List<Volume> volumes = VolumeCli.parse(line);

        Assert.assertEquals(1, volumes.size());
        Assert.assertEquals(volume, volumes.get(0));
    }

    @Test
    public void getAllTest() {
        Assert.assertFalse(VolumeCli.getAllVolumes().isEmpty());
    }

    @Test
    public void getByIdTest() {
        Optional<Volume> opVolume = VolumeCli.getVolumeById(1);

        Assert.assertTrue(opVolume.isPresent());
        Assert.assertEquals("mapr.cldb.internal", opVolume.get().getName());
        Assert.assertFalse(VolumeCli.getVolumeById(-1).isPresent());
    }

    @Test
    public void getByNameTest() {
        Optional<Volume> opVolume = VolumeCli.getVolumeByName("mapr.cluster.root");

        Assert.assertTrue(opVolume.isPresent());
        Assert.assertEquals("/", opVolume.get().getPath());
        Assert.assertFalse(VolumeCli.getVolumeByName("INVALID_NAME").isPresent());
    }

}
