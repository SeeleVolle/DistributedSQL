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
     * Listen to master node
     */
    public void listenMaster() {
        String path = ZkConfigs.generateRegionPath(regionId) + Paths.MASTER.getPath();
        try {
            masterListener = new NodeCache(zkClient, path);
            masterListener.getListenable().addListener(() -> {
                try {
                    // master 已存在/被创建
                    ChildData childData = masterListener.getCurrentData();
                    String master = new String(childData.getData());
                    regionMetadata.setMaster(master);
                    logger.info("New master {} at {}.", master, path);
                } catch (Exception e) {
                    // master 被删除
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
     * Listen to slaves node
     */
    public void listenSlaves() {
        String path = ZkConfigs.generateRegionPath(regionId) + Paths.SLAVE.getPath();
        try {
            slavesListener = new TreeCache(zkClient, path);
            slavesListener.getListenable().addListener((curator, event) -> {
                if (event.getType() == TreeCacheEvent.Type.NODE_ADDED && !event.getData().getPath().equals(path)) {
                    String connectStr = new String(event.getData().getData());
                    regionMetadata.addSlave(connectStr);
                    logger.info("New slave {} at {} is added", connectStr, path);
                } else if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED) {
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
     * Listen to tables node
     */
    public void listenTables() {
        String path = ZkConfigs.generateRegionPath(regionId) + Paths.TABLE.getPath();
        try {
            tablesListener = new TreeCache(zkClient, path);
            tablesListener.getListenable().addListener((curator, event) -> {
                if (event.getType() == TreeCacheEvent.Type.NODE_ADDED && !event.getData().getPath().equals(path)) {
                    String[] paths = event.getData().getPath().split("/");
                    String tableName = paths[3];
                    regionMetadata.addTable(tableName);
                    logger.info("New table {} at {} is added", tableName, path);
                } else if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED) {
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
     * Close all listeners
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
