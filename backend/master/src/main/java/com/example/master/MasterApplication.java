package com.example.master;

import com.example.master.utils.FileLoader;
import com.example.master.zookeeper.Metadata;
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
import java.util.List;

import static com.example.master.zookeeper.ZkConfigs.HOTPOINT_THRESHOLD;

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
        Metadata metadata = Metadata.getInstance();
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, writableRegionCount = 0;
        Metadata.RegionMetadata maxRegion = null, minRegion = null;
        for (var regionMeta : metadata.getData()) {
            if (regionMeta.getVisitCount() < min && regionMeta.isWritable()) {
                writableRegionCount++;
                min = regionMeta.getVisitCount();
                minRegion = regionMeta;
            }
            if (regionMeta.getVisitCount() > max && regionMeta.isWritable()) {
                writableRegionCount++;
                max = regionMeta.getVisitCount();
                maxRegion = regionMeta;
            }
        }
        logger.info("Max visit count is {}, min visit count is {}", max, min);

        // 符合热点迁移条件，将Max Region中的半数数据表迁移至Min Region
        if ((maxRegion != null && minRegion != null) && (writableRegionCount >= 2 && max > 2 * min && max > HOTPOINT_THRESHOLD)) {
            logger.info("Hot point synchronisation is processing");
            List<String> tablesToMove = maxRegion.getTables().subList(0, maxRegion.getTables().size() / 2);
            requestSyncTables(maxRegion.getMaster(), minRegion.getMaster(), tablesToMove);
            for (var region : metadata.getData()) {
                region.setZeroVisitCount();
            }
            logger.info("Hot point synchronisation completed");
        } else {
            logger.info("No hot point synchronisation is required");
        }
    }

    public void requestSyncTables(String from, String to, List<String> tables) {

    }

}
