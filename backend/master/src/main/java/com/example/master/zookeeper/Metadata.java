package com.example.master.zookeeper;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Vector;

@Getter @Setter
public class Metadata {

    @Data
    public static class RegionMetadata {
        private String master; // 区域内主节点的HostName
        private List<String> tables; // 区域内负责存储的表
        private List<String> slaves; // 区域内从节点的HostName
    }

    private static Metadata metadata;

    public static Metadata getInstance() {
        if (metadata == null) {
            metadata = new Metadata();
        }
        return metadata;
    }

    private List<RegionMetadata> data;
    private Metadata() {
        this.data = new Vector<>();
    }

}