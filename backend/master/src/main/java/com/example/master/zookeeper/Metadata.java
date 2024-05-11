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
        volatile private String master; // 区域内主节点的HostName
        volatile private List<String> tables; // 区域内负责存储的表
        volatile private List<String> slaves; // 区域内从节点的HostName
        // private int number; // 暂时没有用处
        volatile private int visitCount;


        public RegionMetadata() {
            this.tables = new Vector<>();
            this.slaves = new Vector<>();
            this.visitCount = 0;
            this.master = "";
        }

        synchronized public void incrementVisitCount() {
            this.visitCount++;
        }

        synchronized public void setZeroVisitCount() {
            this.visitCount = 0;
        }

        /**
         * 返回region是否存在表tableName
         *
         * @param tableName 表名
         * @return 返回region是否存在表tableName
         */
        synchronized public Boolean hasTable(String tableName) {
            return tables.contains(tableName);
        }

        /**
         * @return 只要master不为空，region就是可写的
         */
        synchronized public Boolean isWritable() {
            return !master.isEmpty();
//            return !slaves.isEmpty() || !master.isEmpty();
        }

        synchronized public void addSlave(String slave) {
            if (slaves.contains(slave)) {
                logger.warn("Slave '{}' already exists", slave);
            } else {
                slaves.add(slave);
            }
        }

        synchronized public void removeSlave(String slave) {
            if (!slaves.contains(slave)) {
                logger.warn("Slave '{}' doesn't exist", slave);
            } else {
                slaves.remove(slave);
            }
        }

        synchronized public void addTable(String table) {
            if (tables.contains(table)) {
                logger.warn("Table '{}' already exists", table);
            } else {
                tables.add(table);
            }
        }

        synchronized public void removeTable(String table) {
            if (!tables.contains(table)) {
                logger.warn("Table '{}' doesn't exist", table);
            } else {
                tables.remove(table);
            }
        }

        volatile private int slaveIdx = 0;

        /**
         * 轮询选择Region内下一个被调用的Slave服务器，如果没有slaves返回master的地址
         *
         * @return 返回Slave的Hostname
         */
        synchronized public String pickQueryServer() {
            String hostName;
            if (!slaves.isEmpty()) {
                slaveIdx = (slaveIdx + 1) % slaves.size();
                hostName = slaves.get(slaveIdx);
            } else if (!master.isEmpty()) {
                hostName = master;
            } else {
                hostName = "null";
                logger.warn("No slave server is active and master can't be used");
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
     * 遍历所有Region检查每个Region看是否存在指定Table
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

    /**
     * 遍历每个Region检查该Region是否符合可写条件
     *
     * @return 返回真值
     */
    public Boolean hasWritable() {
        for (var i : data) {
            if (i.isWritable()) {
                return true;
            } else {
                logger.info("Region with master {} is not writable", i.master);
            }
        }
        logger.warn("No writable table exists");
        return false;
    }

    public enum OperationType {
        CREATE_TABLE,
        DROP_TABLE,
        QUERY_TABLE,
        WRITE_TABLE
    }

    /**
     * 遍历所有Region，根据数据库的操作类型，做出对应的判断与检查，选择合适的服务器
     *
     * @param tableName 操作的表
     * @param type      操作类型，可以是QUERY_TABLE, CREATE_TABLE, WRITE_TABLE(SQL的INSERT, UPDATE, DROP...写操作都属于WRITE_TABLE)
     * @return 返回被选择的服务器IP地址
     */
    public String pickServer(String tableName, OperationType type) {
        String hostName = "null";
        switch (type) {
            case QUERY_TABLE -> {
                for (var region : data) {
                    if (region.hasTable(tableName)) {
                        hostName = region.pickQueryServer(); // 轮询region内的服务器来处理查询
                        region.incrementVisitCount();
                    }
                }
            }
            case CREATE_TABLE -> {
                int minNTables = Integer.MAX_VALUE;
                RegionMetadata minRegion = null;
                for (var region : data) {
                    if (region.isWritable()) {
                        int nTables = region.getTables().size();
                        // 选择table最少的region来创建表
                        if (nTables < minNTables) {
                            minNTables = nTables;
                            hostName = region.getMaster(); // 由master去下达指令给region的所有服务器
                            minRegion = region;
                        }
                    } else {
                        logger.warn("Region with master {} is not writable", region.master);
                    }
                }
                if (minRegion != null) {
                    minRegion.incrementVisitCount();
                }
            }
            case WRITE_TABLE -> {
                for (var region : data) {
                    if (region.hasTable(tableName)) {
                        hostName = region.getMaster(); // 由master去下达指令给region的所有服务器
                        region.incrementVisitCount();
                    } else {
                        logger.warn("Region with master {} doesn't have table {}", region.master, tableName);
                    }
                }
            }
            default -> logger.warn("No server being picked");

        }

        return hostName;
    }
}
