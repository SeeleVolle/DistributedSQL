package com.example.master;

import com.example.master.utils.PersistenceHandler;
import com.example.master.zookeeper.Metadata;
import com.example.master.zookeeper.ZkClient;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.client.RestTemplate;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static com.example.master.zookeeper.ZkConfigs.HOTPOINT_THRESHOLD;

@SpringBootApplication
@EnableScheduling
public class MasterApplication {
    public static final Logger logger = LoggerFactory.getLogger(MasterApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(MasterApplication.class, args);
    }

    @Value("${master.config}")
    private String configDir;

    private ZkClient zkClient;

    @PostConstruct
    private void init() {
        try {
            logger.info("zkServers config file: {}", configDir);
            PersistenceHandler.loadConfigurations(configDir);
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
        logger.info("Successfully cleaned up, disconnected from Zookeeper");
    }

    /**
     * 热点检测，热点需要符合以下几个条件
     * 1. 可写Region的数量>=2
     * 2. 被访问最多的Region的访问次数是最少的Region的次数的两倍以上
     * 3.
     * 4.
     */
    @Scheduled(fixedRate = 10000)
    public void hotPointChecker() {
        logger.info("Checking hot point");
        Metadata metadata = Metadata.getInstance();
        if (metadata.getIsMaster()) {
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, writableRegionCount = 0;
            Metadata.RegionMetadata maxRegion = null, minRegion = null;
            for (var regionMeta : metadata.getRegions()) {
                if (regionMeta.getVisitCount() > max && regionMeta.isWritable()) {
                    writableRegionCount++;
                    max = regionMeta.getVisitCount();
                    maxRegion = regionMeta;
                }
            }
            if (maxRegion != null) {
                for (var regionMeta : metadata.getRegions()) {
                    Sets.SetView<Metadata.RegionMetadata.Table> intersection = Sets.intersection(maxRegion.getTables(), regionMeta.getTables());
                    if (regionMeta.getVisitCount() < min && regionMeta.isWritable() && intersection.isEmpty()) {
                        writableRegionCount++;
                        min = regionMeta.getVisitCount();
                        minRegion = regionMeta;
                    }
                }
                logger.info("Max visit count is {}, min visit count is {}", max, min);
            } else {
                logger.info("No max region found, can't process hot point synchronization");
            }

            // 符合热点迁移条件，将Max Region中的半数数据表迁移至Min Region
            if ((maxRegion != null && minRegion != null) && (writableRegionCount >= 2 && max > 2 * min && max > HOTPOINT_THRESHOLD)) {
                logger.info("Hot point synchronisation is processing");
                Set<Metadata.RegionMetadata.Table> tablesToMove = maxRegion.getTables();
                requestSyncTables(maxRegion.getMaster(), minRegion.getMaster(), tablesToMove);
                for (var region : metadata.getRegions()) {
                    region.setZeroVisitCount();
                }
                logger.info("Hot point synchronisation completed, all regions visit count is reset");
            } else {
                logger.info("No hot point synchronisation is required");
            }
        } else {
            logger.info("I'm not Master-Master, no responsibility for hot point synchronisation");
        }

    }

    public void requestSyncTables(String maxRegion, String minRegion, Set<Metadata.RegionMetadata.Table> tables) {
        List<Metadata.RegionMetadata.Table> maxRegionTables = new Vector<>(), minRegionTables = new Vector<>();
        for (var table : tables) {
            String tableName = table.getTableName();
            int start = table.getStart(), end = table.getEnd();
            if (end - start > 1) {
                int mid = (start + end) / 2;
                Metadata.RegionMetadata.Table
                        maxT = new Metadata.RegionMetadata.Table(start, mid, tableName),
                        minT = new Metadata.RegionMetadata.Table(mid, end, tableName);
                maxRegionTables.add(maxT);
                minRegionTables.add(minT);
            } else {
                logger.warn("Partition size is already minimal, no repartition on table {} is required", tableName);
            }
        }
        for (var t : maxRegionTables) {
            logger.info("Max: {}", t);
        }
        for (var t : minRegionTables) {
            logger.info("Min: {}", t);
        }

        try {
            RestTemplate rt = new RestTemplate();
            String requestUrl = "http://" + maxRegion + "/hotsend?targetIP=" + minRegion;
            logger.info("{}", requestUrl);
            String r = rt.postForObject(requestUrl, minRegionTables, String.class);
            logger.info("Hotpoint synchronization result: {}", r);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
