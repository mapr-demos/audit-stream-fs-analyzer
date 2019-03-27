package com.mapr.demo.auditstream.db;

import com.mapr.demo.auditstream.db.entity.FileInfo;
import com.mapr.demo.auditstream.db.entity.Volume;
import com.mapr.demo.auditstream.db.repository.FileInfoRepository;
import com.mapr.demo.auditstream.db.repository.VolumeRepository;
import com.mapr.springframework.data.maprdb.core.MapROperations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.SortOrder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class FileInfoDatabaseService {
    private final static int DEFAULT_PAGE_SIZE = 20;

    private final FileInfoRepository fileInfoRepository;
    private final VolumeRepository volumeRepository;
    private final MapROperations operations;

    public List<FileInfo> getAllRecords() {
        return resolvePath(fileInfoRepository.findAll());
    }

    public Page<FileInfo> getRecordsPageByParameters(Integer page, Integer size, String lastFid, Boolean previous,
                                                     String fid, String name, String path, Long accessDate,
                                                     Long updateDate) {
        int actualPage = page == null || lastFid == null ? 0 : page;
        int actualSize = size == null ? DEFAULT_PAGE_SIZE : size;

        QueryCondition queryCondition = getQueryCondition(fid, name, path, accessDate, updateDate, lastFid, previous);
        List<FileInfo> result = resolvePath(operations.execute(getQuery(queryCondition, previous, actualSize),
                FileInfo.class));

        if (previous != null && previous)
            result.sort(Comparator.comparing(FileInfo::getFid));

        return convertToPage(result, actualPage, actualSize, previous);
    }

    QueryCondition getQueryCondition(String fid, String name, String path, Long accessDate, Long updateDate,
                                     String lastFid, Boolean previous) {
        QueryCondition condition = operations.getConnection().newCondition();

        if (fid != null)
            formConditionBlockIfNecessary(condition).matches("_id", fid);

        if (name != null)
            formConditionBlockIfNecessary(condition).matches("name", name);

        if (path != null)
            formConditionBlockIfNecessary(condition).matches("path", path);

        if (accessDate != null)
            formConditionBlockIfNecessary(condition)
                    .is("accessDate", QueryCondition.Op.LESS, accessDate);

        if (updateDate != null)
            formConditionBlockIfNecessary(condition)
                    .is("updateDate", QueryCondition.Op.LESS, updateDate);

        if (lastFid != null)
            formConditionBlockIfNecessary(condition)
                    .is("_id", previous != null && previous ? QueryCondition.Op.LESS : QueryCondition.Op.GREATER,
                            lastFid);

        if (!condition.isEmpty())
            condition.close();

        condition.build();
        log.debug("Condition request {}", condition.toString());

        return condition;
    }

    Query getQuery(QueryCondition queryCondition, Boolean previous, int pageSize) {
        Query query = operations.getConnection().newQuery().where(queryCondition);

        query.limit(pageSize + 1);
        if (previous != null && previous)
            query.orderBy("_id", SortOrder.DESC);

        query.build();
        log.debug("Query request {}", query.toString());

        return query;
    }

    Page<FileInfo> convertToPage(List<FileInfo> records, Integer page, Integer size, Boolean previous) {
        List<FileInfo> resultList;
        if (previous != null && previous)
            resultList = records.subList(records.size() > 0 ? 1 : 0, records.size());
        else
            resultList = records.subList(0, size < records.size() ? size : records.size());
        return new PageImpl<>(resultList, PageRequest.of(page, size), page * size + records.size());
    }

    private QueryCondition formConditionBlockIfNecessary(QueryCondition queryCondition) {
        return queryCondition.isEmpty() ? queryCondition.and() : queryCondition;
    }

    private List<FileInfo> resolvePath(List<FileInfo> fileList) {
        List<String> volumesId = fileList.stream().map(FileInfo::getVolumeId).distinct().collect(Collectors.toList());
        Map<String, String> pathMap = volumeRepository.findAllById(volumesId).stream()
                .collect(Collectors.toMap(Volume::getId, Volume::getPath));

        return fileList.stream().peek(f -> f.setPath(resolvePath(f.getPath(), pathMap.get(f.getVolumeId()))))
                .collect(Collectors.toList());
    }

    private String resolvePath(String filePath, String volumePath) {
        if (filePath != null && volumePath != null)
            return String.format("%s%s", volumePath, filePath);
        return filePath;
    }

}
