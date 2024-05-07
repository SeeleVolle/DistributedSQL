package utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;

/**
 * @projectName: region
 * @package: utils
 * @className: zookeeper
 * @author: Huang Jinjun
 * @description: TODO
 * @date: 2024/5/7 11:00
 * @version: 1.0
 */

@
public class Zookeeper {

    private String localaddr;
    private String zkServerAddr;
    private CuratorFramework client;
    private DataSource dataSource;
    private Integer maxRegions;
    private Integer maxServers;

    private Boolean isMaster;
    //regionID是本Server所属的Region的ID
    private Integer regionID;
    //serverID是本Server在对应Region中的ID，通过/regiin+regionID/server+serverID来唯一标识Zookeeper中的一个节点
    private Integer serverID;


    public Zookeeper(String localaddr, String zkServerAddr, DataSource dataSource, Integer maxRegions, Integer maxServers) {
        this.localaddr = localaddr;
        this.zkServerAddr = zkServerAddr;
        this.dataSource = dataSource;
        this.isMaster = false;
        this.maxRegions = maxRegions;
        this.maxServers = maxServers;
    }

    public void connect(){
        System.out.println("Region server " + localaddr + " is connecting to zkServer: "+ zkServerAddr + " ......");
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddr)
                .retryPolicy(new ExponentialBackoffRetry(2000, 5))
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(10000)
                .build();
        client.start();
        System.out.println("Connected successfully");
        initzk();
    }

    //初始化本Region Server在Zookeeper中的
    public void initzk(){
        System.out.println("initializing zk ......");
        //寻找空闲的Region，将本RegionServer添加到Zookeeper中
        for(int i = 0; i < maxRegions; i++){
            try{
                //如果该Region不存在master，则就成为master
                if(client.checkExists().forPath("/region" + i + "/master") == null){
                    regionID = i;
                    client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID);
                    client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/master", localaddr.getBytes());
                    client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/number", "1".getBytes());
                    this.isMaster = true;
                    WriteTableMeta();
                    break;
                }
                else{
                    Integer regionnumber = Integer.parseInt(new String(client.getData().forPath("/region" + i + "/number")));
                    if(regionnumber < maxServers){
                        regionID = i;
                        serverID = regionnumber;
                        client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/slave" + serverID, localaddr.getBytes());
                        client.setData().forPath("/region" + regionID + "/number", new Integer(regionnumber + 1).toString().getBytes());
                        break;
                    }
                    else continue;
                }
                if(i == maxRegions - 1){
                    System.out.println("No available region for this server");
                }
            }catch(Exception e){
                System.out.println("Region server " + localaddr + " failed to add to zkserver");
            }
        }
    }

    public void WriteTableMeta(){
        try{
            Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement("show tables");
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                String tableName = rs.getString(1);
                client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/table" + tableName, tableName.getBytes());
            }
        }catch(Exception e){
            System.out.println("Error: Master can't write table meta to zkserver");
        }
    }

    public void CopyFromRemoteTable(){

    }

    public void CopyFromRemoteDB(){

    }

    public void disconnect(){
        System.out.println("Region server " + localaddr + " is disconneting to zkServer: "+ zkServerAddr + " ......");
        if(client != null){
            client.close();
            client = null;
        }
        System.out.println("Disconnected successfully");
    }

}

class MasterListener extends CuratorCacheListener{

}
