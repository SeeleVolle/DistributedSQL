package com.example.master.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

/**
 * ZkClient is a singleton class that manages the connection to Zookeeper and initializes metadata
 */
public class ZkClient {
    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private ZkClient() {
        zkServers = ZkConfigs.zkServers;
        metadata = Metadata.getInstance();
    }

    private static ZkClient instance;

    public static ZkClient getInstance() {
        if (instance == null) {
            instance = new ZkClient();
        }
        return instance;
    }

    // Zookeeper 服务器的地址列表
    private final Vector<String> zkServers;
    private final Metadata metadata;
    private CuratorFramework zkClient;
    private ZkListener zkListener;

    public void init() {
        connect();
        initMetadata();
    }

    private void connect() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        this.zkClient = CuratorFrameworkFactory.newClient(String.join(",", zkServers), 5000, 5000, retryPolicy);
        this.zkClient.start();
    }

    /**
     * Initialize metadata for each region and listen to changes in Zookeeper
     */
    private void initMetadata() {
        for (int i = 0; i < ZkConfigs.MAX_REGION; i++) {
            Metadata.RegionMetadata regionMetadata = new Metadata.RegionMetadata();
            zkListener = new ZkListener(zkClient, i, regionMetadata);
            zkListener.listenMaster(); // Listen to master ZNode
            zkListener.listenTables(); // Listen to tables ZNode and its child nodes
            zkListener.listenSlaves(); // Listen to slaves ZNode and its child nodes
            metadata.getRegions().add(regionMetadata);
        }
    }

    /**
     * 创建ZNode
     *
     * @param path ZNode路径
     * @param data ZNode数据
     * @throws Exception 创建ZNode失败
     */
    public void createZNode(String path, String data) throws Exception {
        if (!pathExist(path)) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        }
    }

    /**
     * 创建无数据节点（默认数据为Client的IP地址）
     *
     * @param path ZNode路径
     * @throws Exception 创建ZNode失败
     */
    public void createZNode(String path) throws Exception {
        if (!pathExist(path)) {
            zkClient.create().creatingParentsIfNeeded().forPath(path);
        }
    }

    /**
     * 删除ZNode
     *
     * @param path ZNode路径
     */
    private Boolean pathExist(String path) {
        boolean isExsit = false;
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            isExsit = stat != null;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return isExsit;
    }

    /**
     * Disconnect from Zookeeper
     */
    public void disconnect() {
        if (zkListener != null) {
            zkListener.close();
            zkListener = null;
        } else {
            logger.info("zkListener is null, no close operation for zkListener is needed.");
        }
        if (zkClient != null) {
            zkClient.close();
            zkClient = null;
        } else {
            logger.info("zkClient is null, no close operation for zkClient is needed.");
        }
    }
}
