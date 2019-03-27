package com.mapr.demo.auditstream.db.entity;

import com.mapr.springframework.data.maprdb.core.mapping.Document;
import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
@Document
public class Volume {
    @Id
    private String id;
    private String name;
    private String path;
}
