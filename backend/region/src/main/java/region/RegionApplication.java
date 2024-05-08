package region;

import jakarta.annotation.PostConstruct;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RestController;
import utils.Zookeeper;

import java.nio.charset.StandardCharsets;

@SpringBootApplication
@RestController
@CrossOrigin
public class RegionApplication {

    @Value("${zookeeper.server}")
    private String zkServerAddr;
    @Value("${region.port}")
    private int port;
    @Value("${region.maxRegions}")
    private int maxRegions;
    @Value("${region.maxServers}")
    private int maxServers;

    private static Zookeeper zookeeper;


    public static void main(String[] args) {
        SpringApplication.run(RegionApplication.class, args);
    }

    @PostConstruct
    void init(){
        System.out.println("Region Server is initializing");

    }

    public void testConnection() throws Exception {
        System.out.println("Test Connection");
        System.out.println("Zookeeper Server: " + zkServerAddr);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddr)
                .retryPolicy(new ExponentialBackoffRetry(2000, 5))
                .sessionTimeoutMs(1000)
                .connectionTimeoutMs(10000)
                .build();
        curatorFramework.start();
        curatorFramework.create().forPath("/test", "Hello".getBytes(StandardCharsets.UTF_8));
    }
}
