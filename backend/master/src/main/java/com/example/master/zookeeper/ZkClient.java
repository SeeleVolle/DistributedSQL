package com.example.master.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

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

    public void init() {
        connect();
        initMetadata();
    }

    private void connect() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        this.zkClient = CuratorFrameworkFactory.newClient(String.join(",", zkServers), 5000, 5000, retryPolicy);
        this.zkClient.start();
    }

    private void initMetadata() {
        for (int i = 0; i < ZkConfigs.MAX_REGION; i++) {
            Metadata.RegionMetadata regionMetadata = new Metadata.RegionMetadata();
            String prefix = String.format("/region_%d", i + 1);
            ZkListener zkListener = new ZkListener(zkClient, prefix, regionMetadata);
            zkListener.listenMaster(); // Listen to master ZNode
            zkListener.listenTables(); // Listen to tables ZNode and its child nodes
            zkListener.listenSlaves(); // Listen to slaves ZNode and its child nodes
            metadata.getData().add(regionMetadata);
        }
    }

    // 创建含数据节点
    public void createZNode(String path, String data) throws Exception {
        if (!pathExist(path)) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        }
    }

    // 创建无数据节点（默认数据为Client的IP地址）
    public void createZNode(String path) throws Exception {
        if (!pathExist(path)) {
            zkClient.create().creatingParentsIfNeeded().forPath(path);
        }
    }

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

    public void disconnect() {
        if (zkClient != null) {
            zkClient.close();
            zkClient = null;
        } else {
            logger.info("zkClient is null, no close operation for zkClient is needed.");
        }
    }
}
