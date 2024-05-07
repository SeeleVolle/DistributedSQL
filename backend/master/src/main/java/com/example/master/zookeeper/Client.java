package com.example.master.zookeeper;

import com.example.master.utils.StaticResourcesLoader;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private Client() {
        zkServers = StaticResourcesLoader.loadZkServers();
    }

    private static Client instance;

    public static Client getInstance() {
        if (instance == null) {
            instance = new Client();
        }
        return instance;
    }

    // Zookeeper 服务器的地址列表
    private final Vector<String> zkServers;
    private CuratorFramework zkClient;

    public void connect() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        this.zkClient = CuratorFrameworkFactory.newClient(String.join(",", zkServers), 5000, 5000, retryPolicy);
        this.zkClient.start();
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
}
