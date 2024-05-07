package com.example.master.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class ZkListener {
    private static final Logger logger = LoggerFactory.getLogger(ZkListener.class);
    private final CuratorFramework zkClient;
    private final String prefix;
    private final Metadata.RegionMetadata regionMetadata;

    /**
     * @param zkClient       Instance of ZkClient
     * @param prefix         It should look like this "/your/path/here"
     * @param regionMetadata
     */
    public ZkListener(CuratorFramework zkClient, String prefix, Metadata.RegionMetadata regionMetadata) {
        this.prefix = prefix;
        this.zkClient = zkClient;
        this.regionMetadata = regionMetadata;
    }

    /**
     * @param prefix         It should look like this "/your/path/here"
     * @param regionMetadata
     */
    public ZkListener(String prefix, Metadata.RegionMetadata regionMetadata) {
        List<String> zkServers = ZkConfigs.zkServers;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        this.zkClient = CuratorFrameworkFactory.newClient(String.join(",", zkServers), 5000, 5000, retryPolicy);
        this.prefix = prefix;
        this.regionMetadata = regionMetadata;
    }

    /**
     *
     */
    public void listenMaster() {
        String path = prefix + "/master";
        try {
            NodeCache nodeCache = new NodeCache(zkClient, path);
            nodeCache.getListenable().addListener(() -> {
                try {
                    // master 已存在/被创建
                    ChildData childData = nodeCache.getCurrentData();
                    String master = new String(childData.getData());
                    regionMetadata.setMaster(master);
                } catch (Exception e) {
                    // master 被删除
                    logger.error(e.getMessage());
                }
            });
            nodeCache.start();
        } catch (Exception e) {
            logger.error("Error occurs on listen master at path: {} ", path);
            logger.error(e.getMessage());
        }
    }

    public void listenSlaves() {
        String path = prefix + "/slaves";
        try {
            TreeCache treeCache = new TreeCache(zkClient, path);
            treeCache.getListenable().addListener((curator, event) -> {
                // TODO: Not yet implement
            });
            treeCache.start();
        } catch (Exception e) {
            logger.error("Error occurs on listen slaves at path: {} ", path);
            logger.error(e.getMessage());
        }
    }

    public void listenTables() {
        String path = prefix + "/tables";
        try {
            TreeCache treeCache = new TreeCache(zkClient, path);
            treeCache.getListenable().addListener((curator, event) -> {
                // TODO: Not yet implement
            });
            treeCache.start();
        } catch (Exception e) {
            logger.error("Error occurs on listen tables at path: {} ", path);
            logger.error(e.getMessage());
        }
    }
}
