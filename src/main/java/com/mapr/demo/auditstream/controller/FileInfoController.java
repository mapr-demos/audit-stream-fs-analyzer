package com.mapr.demo.auditstream.controller;

import com.mapr.demo.auditstream.db.FileInfoDatabaseService;
import com.mapr.demo.auditstream.db.entity.FileInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@Slf4j
@RestController
@RequiredArgsConstructor
public class FileInfoController {
    private final FileInfoDatabaseService databaseService;

    @RequestMapping(path = "/fileinfo", produces = "application/json", method = RequestMethod.GET)
    List<FileInfo> getAllRecords() {
        log.info("Get request for '/fileinfo'");
        return databaseService.getAllRecords();
    }

    @RequestMapping(path = "/fileinfo/paginated", produces = "application/json", method = RequestMethod.GET)
    Page<FileInfo> getPageAllRecords(@RequestParam(value = "page", required = false) Integer page,
                                     @RequestParam(value = "size", required = false) Integer size,
                                     @RequestParam(value = "previous", required = false, defaultValue = "false")
                                             Boolean previous,
                                     @RequestParam(value = "lastFid", required = false) String lastFid,
                                     @RequestParam(value = "fid", required = false) String fid,
                                     @RequestParam(value = "name", required = false) String name,
                                     @RequestParam(value = "path", required = false) String path,
                                     @RequestParam(value = "accessDate", required = false) Long accessDate,
                                     @RequestParam(value = "updateDate", required = false) Long updateDate) {

        log.info("Get request for '/fileinfo/paginated'");
        log.debug("page={}, size={}, lasFid={}, previous={}, fid={}, name={}, path={}, accessDate={}, updateDate={}",
                page, size, lastFid, previous, fid, name, path, accessDate, updateDate);
        return databaseService.getRecordsPageByParameters(page, size, lastFid, previous, fid, name, path, accessDate,
                updateDate);
    }

}
