package com.example.master.zookeeper;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

/**
 * Metadata class is a singleton class that holds the metadata of the system
 */
@Getter
@Setter
public class Metadata {
    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);

    @Data
    public static class RegionMetadata {
        private static final Logger logger = LoggerFactory.getLogger(RegionMetadata.class);
        private String master; // 区域内主节点的HostName
        private List<String> tables; // 区域内负责存储的表
        private List<String> slaves; // 区域内从节点的HostName
        private int number;

        public RegionMetadata() {
            this.tables = new Vector<>();
            this.slaves = new Vector<>();
        }

        public Boolean hasTable(String tableName) {
            return tables.contains(tableName);
        }

        public Boolean isWritable() {
            return !slaves.isEmpty() && !master.isEmpty();
        }

        private int slaveIdx = 0;

        /**
         * 轮询选择Region内下一个被调用的Slave服务器
         *
         * @return 返回Slave的Hostname
         */
        public String pickServer() {
            String hostName = "";
            if (!slaves.isEmpty()) {
                slaveIdx = (slaveIdx + 1) % slaves.size();
                hostName = slaves.get(slaveIdx);
            } else if (!master.isEmpty()) {
                hostName = master;
            } else {
                hostName = "null";
                logger.error("No slave server is active and master can't be used");
            }
            logger.info("Server being chosen is '{}'", hostName);
            return hostName;
        }
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

    /**
     * 检查所有Region看是否存在Table
     *
     * @param tableName Table name
     * @return True if table existing in one of the region, else return false
     */
    public Boolean hasTable(String tableName) {
        for (var i : data) {
            if (i.hasTable(tableName)) return true;
        }
        return false;
    }

    public Boolean hasWritable() {
        for (var i : data) {
            if (i.isWritable()) return true;
        }
        return false;
    }

    public enum OperationType {
        CREATE_TABLE,
        DROP_TABLE,
        QUERY_TABLE,
        WRITE_TABLE;
    }

    public String pickServer(String tableName, OperationType type) {
        String hostName = "null";
        int minNTables = Integer.MAX_VALUE;
        for (var region : data) {
            switch (type) {
                case QUERY_TABLE -> {
                    if (region.hasTable(tableName)) {
                        hostName = region.pickServer(); // 轮询region内的服务器来处理查询
                    }
                }
                case CREATE_TABLE -> {
                    if (region.isWritable()) {
                        int nTables = region.getTables().size();

                        // 选择table最少的region来创建表
                        if (nTables < minNTables) {
                            minNTables = nTables;
                            hostName = region.getMaster(); // 由master去下达指令给region的所有服务器
                        }
                    }
                }
                case WRITE_TABLE -> {
                    if (region.hasTable(tableName)) {
                        hostName = region.getMaster(); // 由master去下达指令给region的所有服务器
                    }
                }
                default -> {
                    logger.error("No server being picked");
                }
            }
        }
        return hostName;
    }
}
