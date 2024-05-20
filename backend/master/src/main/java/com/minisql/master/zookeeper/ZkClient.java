package com.minisql.master.zookeeper;

import com.minisql.master.utils.Configs;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ZkClient is a singleton class that manages the connection to Zookeeper and initializes metadata
 */
public class ZkClient {
    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private ZkClient() {
        zkServers = Configs.ZK_SERVERS;
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
    private final List<String> zkServers;
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
        for (int i = 0; i < Configs.MAX_REGION; i++) {
            Metadata.RegionMetadata regionMetadata = new Metadata.RegionMetadata();
            regionMetadata.setRegionId(i);
            zkListener = new ZkListener(zkClient, i, regionMetadata);
            zkListener.listenMaster(); // Listen to master ZNode
            zkListener.listenTables(); // Listen to tables ZNode and its child nodes
            zkListener.listenSlaves(); // Listen to slaves ZNode and its child nodes
            metadata.getRegions().add(regionMetadata);
        }
        zkListener.listenMasterMaster();
        try {
            // TODO
            if (zkClient.checkExists().forPath("/master/master") == null) {
                // If no master-master, this master will be master-master
                initMasterMaster();
            } else {
                // Or else, become master-slave.
                initMasterSlave();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Boolean iAmMasterMaster() {
        return metadata.getIsMaster();
    }

    public void initMasterMaster() {
        logger.info("I am trying to become Master-Master");
        try {
            metadata.setIsMaster(true);
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/master/master");
        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.warn("Failed to become master-master, rolling back");
            metadata.setIsMaster(false);
        }

    }

    public void initMasterSlave() {
        logger.info("I am becoming Master-Slave");
        metadata.setIsMaster(false);
    }


    /**
     * 创建ZNode
     *
     * @param path ZNode路径
     * @param data ZNode数据
     */
    public void createZNode(String path, String data) {
        if (!pathExist(path)) {
            try {
                zkClient.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * 创建无数据节点（默认数据为Client的IP地址）
     *
     * @param path ZNode路径
     */
    public void createZNode(String path) {
        if (!pathExist(path)) {
            try {
                zkClient.create().creatingParentsIfNeeded().forPath(path);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * 删除ZNode
     *
     * @param path ZNode路径
     */
    private Boolean pathExist(String path) {
        boolean exist = false;
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            exist = stat != null;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return exist;
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
            logger.info("Connection to Zookeeper has closed");
        } else {
            logger.info("zkClient is null, no close operation for zkClient is needed.");
        }
    }
}
