package com.example.master.zookeeper;

import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static com.example.master.zookeeper.ZkConfigs.MAX_HASH;

/**
 * Metadata class is a singleton class that holds the metadata of the system
 */
@Getter
@Setter
public class Metadata {
    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);

    @Data
    public static class RegionMetadata {

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        public static class Table {
            private Integer start;
            private Integer end;
            private String tableName;
        }

        private static final Logger logger = LoggerFactory.getLogger(RegionMetadata.class);

        public static Integer hash(String value) {
            return value.hashCode() % MAX_HASH;
        }

        volatile private String master; // 区域内主节点的HostName
        volatile private Set<Table> tables; // 区域内负责存储的表
        volatile private List<String> slaves; // 区域内从节点的HostName
        volatile private int visitCount;


        public RegionMetadata() {
            this.tables = new HashSet<>();
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
            for (Table table : tables) {
                if (table.tableName.equals(tableName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @param tableName 表名
         * @param pkValue   主键的实际值
         * @return 返回这个pkValue的记录是否属于这个Region
         */
        synchronized public Boolean pkValueBelongThisRegion(String tableName, String pkValue) {
            for (Table table : tables) {
                if (table.tableName.equals(tableName)) {
                    Integer hashValue = hash(pkValue);
                    if (hashValue < table.end && hashValue >= table.start) {
                        return true;
                    }
                }
            }
            return false;
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

        synchronized public void addTable(String table, int hashStart, int hashEnd) {
            if (hasTable(table)) {
                logger.warn("Table '{}' already exists", table);
            } else {
                Table t = new Table();
                t.tableName = table;
                t.start = hashStart; // Inclusive
                t.end = hashEnd; // Exclusive
                logger.info("{}", t);
                tables.add(t);
            }
        }

        synchronized public void removeTable(String table) {
            if (!hasTable(table)) {
                logger.warn("Table '{}' doesn't exist", table);
            } else {
                tables.removeIf(t -> t.tableName.equals(table));
            }
        }

        volatile private int slaveIdx = 0;

        /**
         * 轮询选择Region内下一个被调用的Slave服务器，如果没有slaves返回master的地址
         *
         * @return 返回Slave的Hostname
         */
        synchronized public String pickHandleSlave() {
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

    private List<RegionMetadata> regions;
//    private Set<String> masterSlaves;
    private Boolean isMaster;

    private Metadata() {
        this.regions = new Vector<>();
//        this.masterSlaves = new HashSet<>();
        this.isMaster = false;
    }

    /**
     * 遍历所有Region检查每个Region看是否存在指定Table
     *
     * @param tableName Table name
     * @return True if table existing in one of the region, else return false
     */
    public Boolean hasTable(String tableName) {
        for (var i : regions) {
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
        for (var i : regions) {
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
        INSERT_TABLE,
        UPDATE_TABLE,
        DELETE_TABLE,
        WRITE_TABLE
    }

    /**
     * 遍历所有Region，根据数据库的操作类型，做出对应的判断与检查，选择合适的服务器
     *
     * @param tableName 操作的表
     * @param type      操作类型，可以是QUERY_TABLE, CREATE_TABLE, WRITE_TABLE(SQL的INSERT, UPDATE, DROP...写操作都属于WRITE_TABLE)
     * @return 返回被选择的服务器IP地址
     */
    public List<String> pickServer(String tableName, OperationType type, String pkValue) {
        List<String> hostName = new Vector<>();
        switch (type) {
            case DELETE_TABLE:
            case DROP_TABLE:
            case UPDATE_TABLE:
                for (var region : regions) {
                    if (region.hasTable(tableName)) {
                        hostName.add(region.getMaster()); // 轮询region内的服务器来处理查询
                        region.incrementVisitCount();
                    }
                }
                break;
            case QUERY_TABLE:
                // 返回存在被查询的表所在的所有Regions' slaves
                for (var region : regions) {
                    if (region.hasTable(tableName)) {
                        hostName.add(region.pickHandleSlave()); // 轮询region内的服务器来处理查询
                        region.incrementVisitCount();
                    }
                }
                break;
            case CREATE_TABLE:
                // 选择table最少且无此table的region来创建表
                int minNTables = Integer.MAX_VALUE;
                RegionMetadata minRegion = null;
                for (var region : regions) {
                    if (region.isWritable()) {
                        int nTables = region.getTables().size();
                        if (nTables < minNTables) {
                            minNTables = nTables;
                            minRegion = region;
                        }
                    } else {
                        logger.warn("Region with master {} is not writable", region.master);
                    }
                }
                if (minRegion != null) {
                    hostName.add(minRegion.getMaster()); // 由master去下达指令给region的所有服务器
                    minRegion.incrementVisitCount();
                }
                break;

            case INSERT_TABLE:
                // 返回负责处理insert的Region Master
                for (var region : regions) {
                    if (region.pkValueBelongThisRegion(tableName, pkValue)) {
                        hostName.add(region.getMaster());
                        region.incrementVisitCount();
                    } else {
                        logger.warn("{} in table {} doesn't belong to region with master {}", pkValue, tableName, region.master);
                    }
                }
                break;

            default:
                logger.warn("No server being picked");

        }

        return hostName;
    }
}
