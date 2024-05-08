package utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.exit;

/**
 * @projectName: region
 * @package: utils
 * @className: zookeeper
 * @author: Huang Jinjun
 * @description: TODO
 * @date: 2024/5/7 11:00
 * @version: 1.0
 */

public class Zookeeper {

    private final String localaddr;
    private final String zkServerAddr;
    private CuratorFramework client;
    //数据库连接类
    private DatabaseConnection databaseConnection;
    private final Integer maxRegions;
    private final Integer maxServers;

    private MasterListener masterListener;

    //master目录监听器
    private Boolean isMaster;
    private Boolean isReady;
    //regionID是本Server所属的Region的ID
    private Integer regionID;
    //serverID是本Server在对应Region中的ID，通过/regiin+regionID/server+serverID来唯一标识Zookeeper中的一个节点
    private Integer serverID;


    public Zookeeper(String localaddr, String zkServerAddr, DatabaseConnection databaseConnection, Integer maxRegions, Integer maxServers) {
        this.localaddr = localaddr;
        this.zkServerAddr = zkServerAddr;
        this.databaseConnection = databaseConnection;
        this.isMaster = false;
        this.isReady = false;
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
        try{
            String testStr = new String(client.getData().forPath("/test"));
            if(!testStr.equals("hello")){
                System.out.println("Error: Zookeeper connection failed");
                exit(1);
            }
            System.out.println("Connected successfully");
        }catch(Exception e){
            System.out.println("Error: Zookeeper connection failed");
            exit(1);
        }

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
                    masterInit(i, localaddr);
                    break;
                }
                else{
                    Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + i + "/number")));
                    if(number < maxServers){
                        //获取ServerID
                        int serverID = 0;
                        for(int j = 0; j <= number; j++){
                            if(client.checkExists().forPath("/region" + i + "/slave" + j) == null){
                                serverID = j;
                                break;
                            }
                        }
                        slaveInit(i, serverID, localaddr, number);
                        break;
                    }
                }
                if(i == maxRegions - 1){
                    System.out.println("No available region for this server");
                }
            }catch(Exception e){
                System.out.println("Region server " + localaddr + " failed to add to zkserver");
            }
        }
        isReady = true;
    }

    public void masterInit(Integer regionID, String localaddr) throws Exception {
        //1. 创建zookeeper目录
        this.regionID = regionID;
        this.isMaster = true;
        client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID);
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/master", localaddr.getBytes());
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/number", "1".getBytes());
        //2. 拷贝TableMeta到zk中
        WriteTableMeta();
    }

    public void masterUpdated(Integer regionID, Integer serverID, String localaddr) throws Exception {
        //1. 更新zk中的master信息，number信息, 以及子目录信息
        Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/master", localaddr.getBytes());
        client.setData().forPath("/region" + regionID + "/number", Integer.valueOf(number - 1).toString().getBytes());
        client.delete().forPath("/region" + regionID + "/slave" + serverID);
        //2. 停止对master目录的监听
        masterListener.stoplistening();
    }

    public void slaveInit(Integer regionID, Integer serverID, String localaddr, Integer number) throws Exception {
        this.regionID = regionID;
        this.serverID = serverID;
        //1.更新Zookeeper目录，将自己写入到对应目录，更新server number
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/slave" + serverID, localaddr.getBytes());
        client.setData().forPath("/region" + regionID + "/number", Integer.valueOf(number + 1).toString().getBytes());
        //2.从master处拷贝数据
        this.isMaster = false;
        String masterAddr = new String(client.getData().forPath("/region" + regionID + "/master"));
        CopyFromRemoteDB(masterAddr);
        //3. 注册master的监听器
        masterListener = new MasterListener();
        masterListener.startlistening();
    }

    public void WriteTableMeta(){
        try{
            client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID  + "/tables");
            Connection conn = databaseConnection.getConnection();
            PreparedStatement ps = conn.prepareStatement("show tables");
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                String tableName = rs.getString(1);
                client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/tables/" + tableName, tableName.getBytes());
            }
        }catch(Exception e){
            System.out.println("Error: Master can't write table meta to zkserver");
        }
    }

    public void addTable(String name){
        try{
            client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/tables/" + name, name.getBytes());
        }catch(Exception e){
            System.out.println("Error: Master can't add table information to zkserver");
        }
    }

    public boolean isTableExist(String name){
        try{
            if(client.checkExists().forPath("/region" + regionID + "/tables/" + name) != null){
                return true;
            }
        }catch(Exception e){
            System.out.println("Error: Master can't check table information in zkserver");
        }
        return false;
    }

    public void removeTable(String name){
        try{
            client.delete().forPath("/region" + regionID + "/tables/" + name);
        }catch(Exception e){
            System.out.println("Error: Master can't delete table information to zkserver");
        }
    }

    public void CopyFromRemoteTable(String addr, String tablename){
        DatabaseConnection TargetDatabaseConnection = new DatabaseConnection("jdbc:mysql://"+ addr +":3306/DISTRIBUTED", databaseConnection.getUsername(), databaseConnection.getPassword());
        TableCopy tableCopy = new TableCopy(databaseConnection, TargetDatabaseConnection, tablename, tablename);
        tableCopy.copy();
    }

    public void CopyFromRemoteDB(String addr){
        DatabaseConnection TargetDatabaseConnection = new DatabaseConnection("jdbc:mysql://"+ addr +":3306/DISTRIBUTED", databaseConnection.getUsername(), databaseConnection.getPassword());
        DatabaseCopy databaseCopy = new DatabaseCopy(databaseConnection, TargetDatabaseConnection);
        databaseCopy.copy();
    }

    public boolean isMaster(){
        return isMaster;
    }

    public boolean isReady(){
        return isReady;
    }

    public Integer getRegionID(){
        return regionID;
    }

    public Integer getServerID(){
        return serverID;
    }

    public Integer getMaxRegions(){
        return maxRegions;
    }

    public Integer getMaxServers(){
        return maxServers;
    }

    public Integer getServerNumber(){
        try{
            return Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
        }catch(Exception e){
            System.out.println("Error: Can't get server numbers");
        }
        return 0;
    }

    public List<String> getSlaves(){
        try{
            Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
            ArrayList<String> list = new ArrayList<String>();
            for(int i = 0; i < number; i++){
                if(client.checkExists().forPath("/region" + regionID + "/slave" + i) != null){
                    list.add(new String(client.getData().forPath("/region" + regionID + "/slave" + i)));
                }
            }
            return list;
        }catch(Exception e){
            System.out.println("Error: Can't get slave information");
        }
        return null;
    }


    public void close(){
        System.out.println("Region server " + localaddr + " is disconneting to zkServer: "+ zkServerAddr + " ......");
        if(client != null){
            //1. 更新zk信息
            if(isMaster){
                try{
                    client.delete().forPath("/region" + regionID + "/master");
                }catch(Exception e){
                    System.out.println("Error: Master can't delete zkserver info");
                }
            }
            else{
                try{
                    Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
                    client.setData().forPath("/region" + regionID + "/number", Integer.valueOf(number - 1).toString().getBytes());
                    client.delete().forPath("/region" + regionID + "/slave" + serverID);
                }catch(Exception e){
                    System.out.println("Error: Slave can't delete zkserver info");
                }
            }

            //2. 关闭client
            client.close();
            client = null;
        }
        System.out.println("Disconnected successfully");
    }



    class MasterListener {

        private CuratorCache cache;
        MasterListener(){

        }
        public void startlistening(){
            cache = CuratorCache.builder(client, "/region" + regionID + "/master").build();
            cache.listenable().addListener(new MyListener());
            cache.start();
        }

        public void stoplistening(){
            if(cache != null){
                try{
                    cache.close();
                }catch(Exception e){
                    System.out.println("Error: MasterListener can't stop listening");
                }
            }
        }

        class MyListener implements CuratorCacheListener{
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                try{
                    switch (type) {
                        case NODE_CREATED:
                            System.out.println("Master is created");
                            break;
                        case NODE_CHANGED:
                            System.out.println("Master is changed");
                            break;
                        //原master节点下线，尝试成为master
                        case NODE_DELETED:
                            masterUpdated(regionID, serverID, localaddr);
                            break;
                    }
                }catch(Exception e){
                    System.out.println("Error: MasterListener can't handle event");
                }

            }
        }

    }

}


