package com.mapr.demo.auditstream.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.demo.auditstream.db.entity.FileInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DatabaseEntityConvertingTests {
    private final static ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void fileInfoConvertToJson() {
        Map<String, Object> map = new HashMap<>();
        map.put("fid", "123");
        map.put("name", "test");
        map.put("path", "/folder");
        map.put("volumeId", "1");
        map.put("accessDate", 1493290417000L);
        map.put("updateDate", 1493290417000L);

        FileInfo file = new FileInfo();
        file.setFid(map.get("fid").toString());
        file.setName(map.get("name").toString());
        file.setPath(map.get("path").toString());
        file.setVolumeId(map.get("volumeId").toString());
        file.setAccessDate(new Date((long) map.get("accessDate")));
        file.setUpdateDate(new Date((long) map.get("updateDate")));

        Map outputMap = MAPPER.convertValue(file, Map.class);
        Assert.assertEquals(map, outputMap);

        map.put("volumeId", "2");
        Assert.assertNotEquals(map, outputMap);
    }

}
