package com.example.master;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.example.master.api.StatusCode;
import com.example.master.utils.Configs;
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

import static com.example.master.utils.Configs.HOTPOINT_THRESHOLD;

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

    private Integer requestVisitCount(String hostName) {
        RestTemplate rt = new RestTemplate();
        String requestUrl = "http://" + hostName.substring(0, hostName.length() - 4) + Configs.REGION_SERVER_PORT + "/visiting";
        logger.info("Request visit count URL is {}", requestUrl);
        String result = "";
        try {
            result = rt.postForObject(requestUrl, "", String.class);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.info("Response visit count: {}", result);
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject != null) {
            return jsonObject.getIntValue("visitCount");
        }
        return 0;
    }

    private void requestSetZeroVisitCount(String hostName) {
        RestTemplate rt = new RestTemplate();
        String requestUrl = "http://" + hostName.substring(0, hostName.length() - 4) + Configs.REGION_SERVER_PORT + "/visitingClear";
        logger.info("Request clear visit count URL is {}", requestUrl);
        String result = "";
        try {
            result = rt.postForObject(requestUrl, "", String.class);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.info("Response set zero: {}", result);
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
            for (var region : metadata.getRegions()) {
                if (region.isOnline()) {
                    int regionVisitCount = requestVisitCount(region.getMaster());
                    if (regionVisitCount > max) {
                        writableRegionCount++;
                        max = regionVisitCount;
                        maxRegion = region;
                    }
                    region.setVisitCount(regionVisitCount);
                }
            }
            if (maxRegion != null) {
                for (var region : metadata.getRegions()) {
                    Sets.SetView<Metadata.RegionMetadata.Table> intersection = Sets.intersection(maxRegion.getTables(), region.getTables());
                    if (region.getVisitCount() < min && region.isOnline() && intersection.isEmpty()) {
                        writableRegionCount++;
                        min = region.getVisitCount();
                        minRegion = region;
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
                String
                        maxRegionHostName = maxRegion.getMaster().substring(0, maxRegion.getMaster().length() - 4) + Configs.REGION_SERVER_PORT,
                        minRegionHostName = minRegion.getMaster().substring(0, minRegion.getMaster().length() - 4) + Configs.REGION_SERVER_PORT;
                requestSyncTables(maxRegionHostName, minRegionHostName, tablesToMove);
                for (var region : metadata.getRegions()) {
                    requestSetZeroVisitCount(region.getMaster());
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
            logger.info("Move data request URL: {}", requestUrl);
            String r = rt.postForObject(requestUrl, minRegionTables, String.class);
            logger.info("Hotpoint synchronization result: {}", r);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
