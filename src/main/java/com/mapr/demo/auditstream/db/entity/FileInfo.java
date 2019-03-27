package com.mapr.demo.auditstream.db.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.mapr.springframework.data.maprdb.core.mapping.Document;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import java.util.Date;

@Data
@Builder
@Document
@NoArgsConstructor
@AllArgsConstructor
public class FileInfo {
    @Id
    private String fid;
    private String name;
    private String path;
    private String volumeId;

    @Builder.Default
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Date accessDate = new Date(0);

    @Builder.Default
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private Date updateDate = new Date(0);
}
