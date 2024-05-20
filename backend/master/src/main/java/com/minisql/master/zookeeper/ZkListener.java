package com.minisql.master.zookeeper;

import com.minisql.master.utils.Configs;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.minisql.master.utils.Configs.MAX_HASH;

/**
 * Implementation of Zookeeper listener for master server
 */
public class ZkListener {
    private static final Logger logger = LoggerFactory.getLogger(ZkListener.class);
    private final CuratorFramework curatorFramework;
    private final Integer regionId;
    private final Metadata.RegionMetadata regionMetadata;

    /**
     * @param curatorFramework Instance of ZkClient
     * @param regionId         Region ID
     * @param regionMetadata   Instance of RegionMetadata
     */
    public ZkListener(CuratorFramework curatorFramework, Integer regionId, Metadata.RegionMetadata regionMetadata) {
        this.regionId = regionId;
        this.curatorFramework = curatorFramework;
        this.regionMetadata = regionMetadata;
    }

    /**
     * @param regionId       Region ID
     * @param regionMetadata Instance of RegionMetadata
     */
    public ZkListener(Integer regionId, Metadata.RegionMetadata regionMetadata) {
        List<String> zkServers = Configs.ZK_SERVERS;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        this.curatorFramework = CuratorFrameworkFactory.newClient(String.join(",", zkServers), 5000, 5000, retryPolicy);
        this.regionId = regionId;
        this.regionMetadata = regionMetadata;
    }

    private CuratorCache masterMasterListener;

    public void listenMasterMaster() {
        String path = "/master/master";
        try {
            masterMasterListener = CuratorCache.builder(curatorFramework, path).build();
            masterMasterListener.listenable().addListener((type, old, curr) -> {
                try {
                    switch (type) {
                        case NODE_CREATED:
                            String newMasterMaster = new String(curr.getData());
                            logger.info("New Master-Master {}", newMasterMaster);
                            break;
                        case NODE_CHANGED:
                            break;
                        case NODE_DELETED:
                            String oldMasterMaster = new String(old.getData());
                            logger.warn("Master-Master {} has shutdown, I am becoming new Master-Master", oldMasterMaster);
                            ZkClient zkClient = ZkClient.getInstance();
                            zkClient.initMasterMaster();
                            break;

                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            });
            masterMasterListener.start();
            logger.info("MasterMaster is listened at path: {}", path);
        } catch (Exception e) {
            logger.error("Error occurs on listen master-master at path: {} ", path);
            logger.error(e.getMessage());
        }
    }

    private CuratorCache tablesListener;
    private CuratorCache slavesListener;
    private CuratorCache masterListener;

    /**
     * 监听对应Region的master主节点目录
     */
    public void listenMaster() {
        String path = Configs.generateRegionPath(regionId) + Paths.MASTER.getPath();
        try {
            masterListener = CuratorCache.builder(curatorFramework, path).build();
            masterListener.listenable().addListener((type, old, curr) -> {
                try {
                    if (curr != null) {
                        byte[] data = curr.getData();
                        String master = new String(data);
                        regionMetadata.setMaster(master);
                        logger.info("New master {} at {}.", master, path);
                    } else {
                        logger.warn("Master data at {} is missing.", path);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            });
            masterListener.start();
            logger.info("Master is listened at path: {}", path);
        } catch (Exception e) {
            logger.error("Error occurs on listen master at path: {} ", path);
            logger.error(e.getMessage());
            regionMetadata.setMaster("");
        }
    }

    /**
     * 监听对应Region下的slaves从节点，如果有slaves增加、减少、改变，都会被检测到，并实时更新到RegionMetadata中
     */
    public void listenSlaves() {
        String path = Configs.generateRegionPath(regionId) + Paths.SLAVE.getPath();
        try {
            slavesListener = CuratorCache.builder(curatorFramework, path).build();
            slavesListener.listenable().addListener((type, old, curr) -> {
                if (type == CuratorCacheListener.Type.NODE_CREATED && !curr.getPath().equals(path)) {
                    byte[] data = curr.getData();
                    String connectStr = new String(data);
                    regionMetadata.addSlave(connectStr);
                    logger.info("New slave {} at {} is added", connectStr, path);
                } else if (type == CuratorCacheListener.Type.NODE_DELETED && !old.getPath().equals(path)) {
                    // slaveNode 被删除（更新可用数量与路由信息列表）
                    String connectStr = new String(old.getData());
                    regionMetadata.removeSlave(connectStr);
                    logger.info("Slave {} at {} is removed", connectStr, path);
                }
            });
            slavesListener.start();
            logger.info("Slaves are listened at path: {} (Including children)", path);
        } catch (Exception e) {
            logger.error("Error occurs on listen slaves at path: {} ", path);
            logger.error(e.getMessage());
        }
    }

    /**
     * 如同以上监听方法，唯独不同是监听tables的变化
     */
    public void listenTables() {
        String path = Configs.generateRegionPath(regionId) + Paths.TABLE.getPath();
        try {
            tablesListener = CuratorCache.builder(curatorFramework, path).build();
            tablesListener.listenable().addListener((type, old, curr) -> {
                String[] paths;
                switch (type) {
                    case NODE_CHANGED:
                    case NODE_CREATED:
                        paths = curr.getPath().split("/");
                        if (paths.length == 4) {
                            String tableName = paths[3];
                            String hashRange;
                            int hashStart = 0, hashEnd = MAX_HASH;
                            try {
                                hashRange = new String(curr.getData());
                                String[] hrList = hashRange.split(",");
                                hashStart = Integer.parseInt(hrList[0]);
                                hashEnd = Integer.parseInt(hrList[1]);
                                logger.info("Hash range is [{},{})", hashStart, hashEnd);
                            } catch (Exception e) {
                                if (e instanceof NullPointerException) {
                                    logger.warn("Table's node data is null, which mean normal creation of table");
                                } else {
                                    logger.error(e.getMessage());
                                }
                            }
                            if (type == CuratorCacheListener.Type.NODE_CREATED) {
                                regionMetadata.addTable(tableName, hashStart, hashEnd);
                                logger.info("New table {} at {} is added", tableName, path);
                            } else {
                                regionMetadata.updateTable(tableName, hashStart, hashEnd);
                                logger.info("Updated table {} at {}", tableName, path);
                            }
                        }
                        break;
                    case NODE_DELETED:
                        paths = old.getPath().split("/");
                        if (paths.length == 4) {
                            String tableName = paths[3];
                            regionMetadata.removeTable(tableName);
                            logger.info("Table {} at {} is removed", tableName, path);
                        }
                        break;
                }
            });
            tablesListener.start();
            logger.info("Tables are listened at path: {} (Including children) ", path);
        } catch (Exception e) {
            logger.error("Error occurs on listen tables at path: {} ", path);
            logger.error(e.getMessage());
        }
    }

    /**
     * 关闭所有监听。
     */
    public void close() {
        try {
            if (masterMasterListener != null) {
                logger.info("Close master-master listener");
                masterMasterListener.close();
            }
            if (masterListener != null) {
                logger.info("Close master listener");
                masterListener.close();
            }
            if (slavesListener != null) {
                logger.info("Close slaves listener");
                slavesListener.close();
            }
            if (tablesListener != null) {
                logger.info("Close tables listener");
                tablesListener.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
