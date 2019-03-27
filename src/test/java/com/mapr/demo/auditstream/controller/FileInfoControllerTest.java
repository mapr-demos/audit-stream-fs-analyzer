package com.mapr.demo.auditstream.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.demo.auditstream.db.FileInfoDatabaseService;
import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.utils.DataUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class FileInfoControllerTest {
    public final static ObjectMapper MAPPER = new ObjectMapper();
    public final static int LIST_SIZE = 10;

    public MockMvc mvc;

    @Mock
    public FileInfoDatabaseService databaseService;

    @InjectMocks
    public FileInfoController controller;


    @Before
    public void init() {
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void getEmptyListTest() throws Exception {
        Mockito.when(databaseService.getAllRecords()).thenReturn(Collections.emptyList());
        String uri = "/fileinfo";
        MockHttpServletResponse response = mvc.perform(MockMvcRequestBuilders.get(uri).accept(MediaType.APPLICATION_JSON_VALUE))
                .andReturn().getResponse();

        Assert.assertEquals(200, response.getStatus());

        List<FileInfo> result = Arrays.asList(MAPPER.readValue(response.getContentAsByteArray(), FileInfo[].class));
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void getAllTest() throws Exception {
        List<FileInfo> files = DataUtils.genetateFiles(LIST_SIZE);
        Mockito.when(databaseService.getAllRecords()).thenReturn(files);
        String uri = "/fileinfo";
        MockHttpServletResponse response = mvc.perform(MockMvcRequestBuilders.get(uri).accept(MediaType.APPLICATION_JSON_VALUE))
                .andReturn().getResponse();

        Assert.assertEquals(200, response.getStatus());

        List<FileInfo> result = Arrays.asList(MAPPER.readValue(response.getContentAsByteArray(), FileInfo[].class));
        Assert.assertEquals(files.size(), result.size());
        Assert.assertEquals(files, result);
    }

}
