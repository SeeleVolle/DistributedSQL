package com.example.master.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of Zookeeper listener for master server
 */
public class ZkListener {
    private static final Logger logger = LoggerFactory.getLogger(ZkListener.class);
    private final CuratorFramework zkClient;
    private final Integer regionId;
    private final Metadata.RegionMetadata regionMetadata;

    /**
     * @param zkClient       Instance of ZkClient
     * @param regionId       Region ID
     * @param regionMetadata Instance of RegionMetadata
     */
    public ZkListener(CuratorFramework zkClient, Integer regionId, Metadata.RegionMetadata regionMetadata) {
        this.regionId = regionId;
        this.zkClient = zkClient;
        this.regionMetadata = regionMetadata;
    }

    /**
     * @param regionId       Region ID
     * @param regionMetadata Instance of RegionMetadata
     */
    public ZkListener(Integer regionId, Metadata.RegionMetadata regionMetadata) {
        List<String> zkServers = ZkConfigs.zkServers;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        this.zkClient = CuratorFrameworkFactory.newClient(String.join(",", zkServers), 5000, 5000, retryPolicy);
        this.regionId = regionId;
        this.regionMetadata = regionMetadata;
    }

    private TreeCache tablesListener;
    private TreeCache slavesListener;
    private NodeCache masterListener;

    /**
     * 监听对应Region的master主节点目录
     */
    public void listenMaster() {
        String path = ZkConfigs.generateRegionPath(regionId) + Paths.MASTER.getPath();
        try {
            masterListener = new NodeCache(zkClient, path);
            masterListener.getListenable().addListener(() -> {
                try {
                    ChildData childData = masterListener.getCurrentData();
                    byte[] data = childData.getData();
                    if (data != null) {
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
        String path = ZkConfigs.generateRegionPath(regionId) + Paths.SLAVE.getPath();
        try {
            slavesListener = new TreeCache(zkClient, path);
            slavesListener.getListenable().addListener((curator, event) -> {
                if (event.getType() == TreeCacheEvent.Type.NODE_ADDED && !event.getData().getPath().equals(path)) {
                    byte[] data = event.getData().getData();
                    String connectStr = new String(data);
                    regionMetadata.addSlave(connectStr);
                    logger.info("New slave {} at {} is added", connectStr, path);
                } else if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED && !event.getData().getPath().equals(path)) {
                    // slaveNode 被删除（更新可用数量与路由信息列表）
                    String connectStr = new String(event.getData().getData());
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
        String path = ZkConfigs.generateRegionPath(regionId) + Paths.TABLE.getPath();
        try {
            tablesListener = new TreeCache(zkClient, path);
            tablesListener.getListenable().addListener((curator, event) -> {
                if (event.getType() == TreeCacheEvent.Type.NODE_ADDED && !event.getData().getPath().equals(path)) {
                    String[] paths = event.getData().getPath().split("/");
                    String tableName = paths[3];
                    String hashRange;
                    int hashStart = 0, hashEnd = 65536;
                    try {
                        hashRange = new String(event.getData().getData());
                        String[] hrList = hashRange.split(",");
                        hashStart = Integer.parseInt(hrList[0]);
                        hashEnd = Integer.parseInt(hrList[1]);
                    } catch (Exception e) {
                        if (e instanceof NullPointerException) {
                            logger.warn("Table's node data is null, which mean normal creation of table");
                        } else {
                            logger.error(e.getMessage());
                        }
                    }
                    regionMetadata.addTable(tableName, hashStart, hashEnd);
                    logger.info("New table {} at {} is added", tableName, path);
                } else if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED && !event.getData().getPath().equals(path)) {
                    String[] paths = event.getData().getPath().split("/");
                    String tableName = paths[3];
                    regionMetadata.removeTable(tableName);
                    logger.info("Table {} at {} is removed", tableName, path);
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
