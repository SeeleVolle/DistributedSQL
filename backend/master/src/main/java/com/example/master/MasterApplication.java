package com.example.master;

import com.example.master.utils.FileLoader;
import com.example.master.zookeeper.ZkClient;
import com.example.master.zookeeper.ZkConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MasterApplication {
    public static final Logger logger = LoggerFactory.getLogger(MasterApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(MasterApplication.class, args);
        if (args.length > 0) {
            // Load zkServers first
            logger.info("zkServers config file: {}", args[0]);
            ZkConfigs.zkServers = FileLoader.loadZkServers(args[0]);
            ZkClient zkClient = ZkClient.getInstance();
            zkClient.init();
        } else {
            logger.error("Need to specify where is zkServers.mini as an argument");
            System.exit(1);
        }
    }
}
