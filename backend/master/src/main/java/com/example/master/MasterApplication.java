package com.example.master;

import com.example.master.utils.FileLoader;
import com.example.master.zookeeper.ZkClient;
import com.example.master.zookeeper.ZkConfigs;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.net.InetAddress;

@SpringBootApplication
@EnableScheduling
public class MasterApplication {
    public static final Logger logger = LoggerFactory.getLogger(MasterApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(MasterApplication.class, args);
    }

    @Value("${zk.servers}")
    private String zkServersConfDir;
    private ZkClient zkClient;

    @PostConstruct
    private void init() {
        try {
            logger.info("zkServers config file: {}", zkServersConfDir);
            ZkConfigs.zkServers = FileLoader.loadZkServers(zkServersConfDir);
            String ipAddress = InetAddress.getLocalHost().getHostAddress();
            logger.info("Master Server IPv4 address is {}", ipAddress);
            this.zkClient = ZkClient.getInstance();
            this.zkClient.init();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @PreDestroy
    private void cleanup() {
        this.zkClient.disconnect();
        logger.info("Cleanup successfully, disconnected from Zookeeper");
    }

    @Scheduled(fixedRate = 10000)
    public void hotPointChecker() {
        logger.info("Checking hot point");
    }

}
