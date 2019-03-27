package com.mapr.demo.auditstream.db;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.utils.DataUtils;
import com.mapr.springframework.data.maprdb.core.MapROperations;
import com.mapr.springframework.data.maprdb.core.MapRTemplate;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.util.Date;
import java.util.List;

public class FileInfoDatabaseServiceTests {
    public final static int PAGE_SIZE = 10;
    public static FileInfoDatabaseService SERVICE;

    @BeforeClass
    public static void init() {
        MapROperations operations = new MapRTemplate(null, null, null, null);
        SERVICE = new FileInfoDatabaseService(null, null, operations);
    }

    @Test
    public void emptyQueryConditionTest() {
        QueryCondition queryCondition = SERVICE.getQueryCondition(null, null, null, null,
                null, null, null);
        Assert.assertEquals("<EMPTY>", queryCondition.toString());
    }

    @Test
    public void filterByNameTest() {
        String name = "test";
        String query = String.format("((name MATCHES \"%s\"))", name);
        QueryCondition queryCondition = SERVICE.getQueryCondition(null, name, null, null,
                null, null, null);
        Assert.assertEquals(query, queryCondition.toString());
    }

    @Test
    public void filterByDateQueryConditionTest() {
        Long timestamp = new Date().getTime();
        String query = String.format("((accessDate < {\"$numberLong\":%d}) and (updateDate < {\"$numberLong\":%d}))",
                timestamp, timestamp);
        QueryCondition queryCondition = SERVICE.getQueryCondition(null, null, null, timestamp,
                timestamp, null, null);
        Assert.assertEquals(query, queryCondition.toString());
    }

    @Test
    public void previousPageQueryConditionTest() {
        String previousFid = "21.20.31";
        String query = String.format("((_id < \"%s\"))", previousFid);
        QueryCondition queryCondition = SERVICE.getQueryCondition(null, null, null, null,
                null, previousFid, true);
        Assert.assertEquals(query, queryCondition.toString());
    }

    @Test
    public void getQueryTest() {
        int pageSize = 20;
        QueryCondition queryCondition = SERVICE.getQueryCondition(null, null, null, null,
                null, null, null);
        String stringQuery = String.format("OjaiQuery(condition = %s, limit = %d)", queryCondition, pageSize + 1);
        Query query = SERVICE.getQuery(queryCondition, false, 20);
        Assert.assertEquals(stringQuery, query.toString());
    }

    @Test
    public void getPreviousPageQuery() {
        int pageSize = 20;
        QueryCondition queryCondition = SERVICE.getQueryCondition(null, null, null, null,
                null, "20.00.12", true);
        String stringQuery = String.format("OjaiQuery(condition = %s, orderBy = {_id/DESC}, limit = %d)",
                queryCondition, pageSize + 1);
        Query query = SERVICE.getQuery(queryCondition, true, 20);
        Assert.assertEquals(stringQuery, query.toString());
    }

    @Test
    public void convertToNextPageTest() {
        int pageNumber = 2;
        List<FileInfo> files = DataUtils.genetateFiles(PAGE_SIZE + 1);
        Page<FileInfo> page = new PageImpl<>(files.subList(0, PAGE_SIZE), PageRequest.of(pageNumber, PAGE_SIZE),
                (pageNumber + 1) * PAGE_SIZE + 1);
        Page<FileInfo> testPage = SERVICE.convertToPage(files, pageNumber, PAGE_SIZE, false);
        Assert.assertEquals(page, testPage);
    }

    @Test
    public void convertToPreviousPageTest() {
        int pageNumber = 2;
        List<FileInfo> files = DataUtils.genetateFiles(PAGE_SIZE + 1);
        Page<FileInfo> page = new PageImpl<>(files.subList(1, PAGE_SIZE + 1), PageRequest.of(pageNumber, PAGE_SIZE),
                (pageNumber + 1) * PAGE_SIZE + 1);
        Page<FileInfo> testPage = SERVICE.convertToPage(files, pageNumber, PAGE_SIZE, true);
        Assert.assertEquals(page, testPage);
    }

    @Test
    public void convertToLastPageTest() {
        int pageNumber = 2;
        List<FileInfo> files = DataUtils.genetateFiles(PAGE_SIZE - 2);
        Page<FileInfo> page = new PageImpl<>(files, PageRequest.of(pageNumber, PAGE_SIZE),
                (pageNumber + 1) * PAGE_SIZE - 2);
        Page<FileInfo> testPage = SERVICE.convertToPage(files, pageNumber, PAGE_SIZE, false);
        Assert.assertEquals(page, testPage);
    }

}
