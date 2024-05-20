package com.minisql.master;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.minisql.master.utils.Configs;
import com.minisql.master.utils.PersistenceHandler;
import com.minisql.master.zookeeper.Metadata;
import com.minisql.master.zookeeper.ZkClient;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.client.RestTemplate;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static com.minisql.master.utils.Configs.MAX_HOTPOINT_THRESHOLD;
import static com.minisql.master.utils.Configs.MIN_HOTPOINT_THRESHOLD;

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

    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(1000); // 连接超时时间（单位：毫秒）
        factory.setReadTimeout(1000);    // 读取超时时间（单位：毫秒）

        return new RestTemplate(factory);
    }

    @PostConstruct
    private void init() {
        try {
            logger.info("zkServers config file: {}", configDir);
            PersistenceHandler.loadConfigurations(configDir);
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
                            logger.info("Master Server IPv4 on {} is {}", networkInterface.getName(), address.getHostAddress());
                        }
                    }
                }
            } catch (SocketException ex) {
                logger.error(ex.getMessage());
            }
            this.zkClient = ZkClient.getInstance();
            this.zkClient.init();
            logger.info("Initialization completed successfully");
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
        RestTemplate rt = restTemplate();

        String requestUrl = Configs.REGION_SERVER_HTTPS + "://" + hostName.replaceFirst(":[0-9]+", ":" + Configs.REGION_SERVER_PORT) + "/visiting";
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
        RestTemplate rt = restTemplate();
        String requestUrl = Configs.REGION_SERVER_HTTPS + "://" + hostName.replaceFirst(":[0-9]+", ":" + Configs.REGION_SERVER_PORT) + "/visitingClear";
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
        Metadata metadata = Metadata.getInstance();
        if (metadata.getIsMaster()) {
            logger.info("Checking hot point");
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
                    boolean regionHasTable = false;
                    for (var t : maxRegion.getTables()) {
                        if (region.hasTable(t.getTableName())) {
                            regionHasTable = true;
                            break;
                        }
                    }
                    if (region.getVisitCount() < min && region.isOnline() && !regionHasTable) {
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
            if ((maxRegion != null && minRegion != null) && (writableRegionCount >= 2 && max > 2 * min && max > MAX_HOTPOINT_THRESHOLD && min > MIN_HOTPOINT_THRESHOLD)) {
                logger.info("Hot point synchronisation is processing");
                Set<Metadata.RegionMetadata.Table> tablesToMove = maxRegion.getTables();
                String
                        maxRegionHostName = maxRegion.getMaster().replaceFirst(":[0-9]+", ":" + Configs.REGION_SERVER_PORT),
                        minRegionHostName = minRegion.getMaster().replaceFirst(":[0-9]+", ":" + Configs.REGION_SERVER_PORT);
                if (!tablesToMove.isEmpty()) {
                    requestSyncTables(maxRegionHostName, minRegionHostName, tablesToMove, minRegion.getRegionId());
                }
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

    @Data
    private static class Table {
        public Table(String tableName, int targetStart, int targetEnd, int originalStart, int originalEnd) {
            this.tableName = tableName;
            this.targetStart = targetStart;
            this.targetEnd = targetEnd;
            this.originalStart = originalStart;
            this.originalEnd = originalEnd;
        }

        private String tableName;
        private int targetStart;
        private int targetEnd;
        private int originalStart;
        private int originalEnd;
    }

    public void requestSyncTables(String maxRegion, String minRegion, Set<Metadata.RegionMetadata.Table> tables, int targetRegionId) {
        logger.info("Need to move from {} to {} on region {}", maxRegion, minRegion, targetRegionId);
        List<Table> tablesToMove = new Vector<>();
        for (var table : tables) {
            String tableName = table.getTableName();
            int start = table.getStart(), end = table.getEnd();
            if (end - start > 1) {
                int mid = (start + end) / 2;
                tablesToMove.add(new Table(tableName, mid, end, start, mid));
            } else {
                logger.warn("Partition size is already minimal, no repartition on table {} is required", tableName);
            }
        }
        for (var table : tablesToMove) {
            logger.info("Table {} Original[{},{}) Target[{},{})",
                    table.getTableName(),
                    table.getOriginalStart(),
                    table.getOriginalEnd(),
                    table.getTargetStart(),
                    table.getTargetEnd()
            );
        }

        try {
            RestTemplate rt = restTemplate();
            String requestUrl = Configs.REGION_SERVER_HTTPS + "://"
                    + maxRegion.replaceFirst(":[0-9]+", ":" + Configs.REGION_SERVER_PORT)
                    + "/hotsend?targetIP="
                    + minRegion.replaceFirst(":[0-9]+", "")
                    + "&targetRegionID=" + targetRegionId;
            logger.info("Move data request URL: {}", requestUrl);
            String r = rt.postForObject(requestUrl, tablesToMove, String.class);
            logger.info("Hotpoint synchronization result: {}", r);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
