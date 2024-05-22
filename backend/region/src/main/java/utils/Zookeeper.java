package utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

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
    private static final Logger logger = LoggerFactory.getLogger(Zookeeper.class);

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

    static int MAX_HASH = 65536;


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
        logger.info("Region server " + localaddr + " is connecting to zkServer: "+ zkServerAddr + " ......");
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddr)
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(10000)
                .build();
        client.start();
        try{
            String testStr = new String(client.getData().forPath("/test"));
            if(!testStr.equals("hello")){
                logger.error("Error: Zookeeper connection failed");
                exit(1);
            }
            logger.info("Connected successfully");
        }catch(Exception e){
            e.printStackTrace();
            logger.error("Error: Zookeeper connection failed");
            exit(1);
        }

        initzk();
    }

    //初始化本Region Server在Zookeeper中的
    public void initzk(){
        logger.info("initializing zk ......");
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
                        for(int j = 1; j <= number; j++){
                            if(client.checkExists().forPath("/region" + i + "/slaves/slave" + j) == null){
                                serverID = j;
                                break;
                            }
                        }
                        slaveInit(i, serverID, localaddr, number);
                        break;
                    }
                }
                if(i == maxRegions - 1){
                    logger.info("No available region for this server");
                }
            }catch(Exception e){
                e.printStackTrace();
                logger.error("Region server " + localaddr + " failed to add to zkserver");
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
        client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID + "/number", "1".getBytes());
        client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID + "/slaves", localaddr.getBytes());
        client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID  + "/tables");

        //2. 拷贝TableMeta到zk中
        WriteTableMeta();
    }

    public void masterUpdated(Integer regionID, Integer serverID, String localaddr)  {
        try{
            logger.info("Region "+ regionID + " Server "+ serverID +" Trying to be new master...");
            //1. 更新zk中的master信息，number信息, 以及子目录信息
            this.isMaster = true;
            Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
            client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/master", localaddr.getBytes());
            client.delete().forPath("/region" + regionID + "/slaves/slave" + serverID);
            //2. 停止对master目录的监听
            masterListener.stoplistening();
            //3. 更新tables目录
            //WriteTableMeta();
            logger.info("Region "+ regionID + " Server "+ serverID +" is new master");
        } catch(KeeperException.NodeExistsException e){
            logger.info("Region server " + localaddr + " failed to become new master");
        } catch(Exception e){
            e.printStackTrace();
            logger.error("Error: Region server " + localaddr + " failed to update zkserver info");
        }
    }

    public void slaveInit(Integer regionID, Integer serverID, String localaddr, Integer number) throws Exception {
        this.regionID = regionID;
        this.serverID = serverID;
        //1.更新Zookeeper目录，将自己写入到对应目录，更新server number
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/region" + regionID + "/slaves/slave" + serverID, localaddr.getBytes());
        client.setData().forPath("/region" + regionID + "/number", Integer.valueOf(number + 1).toString().getBytes());
        //2.从master处拷贝数据
        this.isMaster = false;
        String masterAddr = getMasterAddr();
        logger.info("Copy from master db: " + masterAddr+ "...");
        CLearDB();
        CopyFromRemoteDB(masterAddr);
        //3. 注册master的监听器
        masterListener = new MasterListener();
        masterListener.startlistening();
    }

    public String getMasterAddr(){
        try{
            return new String(client.getData().forPath("/region" + regionID + "/master"));
        }catch(Exception e){
            logger.error("Error: Can't get master address");
        }
        return null;
    }


    public void CLearDB(){
        try{
            Connection conn = databaseConnection.getConnection();
            PreparedStatement ps = conn.prepareStatement("show tables");
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                String tableName = rs.getString(1);
                logger.info("Clear table: " + tableName);
                ps = conn.prepareStatement("drop table " + tableName);
                ps.executeUpdate();
            }
        }catch(Exception e){
            e.printStackTrace();
            logger.error("Error: Region Server can't clear db");
        }
    }

    public void WriteTableMeta(){
        try{
            Connection conn = databaseConnection.getConnection();
            DatabaseMetaData dbmd = conn.getMetaData();
            PreparedStatement ps = conn.prepareStatement("show tables");
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                String tableName = rs.getString(1);
                logger.info("Write table meta: " + tableName);
                //获取主键哈希范围并写入到Zookeeper中
                String tableHashRange = "";
                ResultSet primaryKeys = dbmd.getPrimaryKeys(null, null, tableName);
                String primaryName = "*";
                if(primaryKeys.next()){
                    primaryName = primaryKeys.getString("COLUMN_NAME");
                }
                PreparedStatement ps_query = conn.prepareStatement(" SELECT " + primaryName + " FROM " + tableName);
//                logger.info(" SELECT " + primaryName + " FROM " + tableName);
                ResultSet primaryps = ps_query.executeQuery();
                int min_range = Integer.MAX_VALUE, max_range = Integer.MIN_VALUE;
//                while(primaryps.next()){
//                    int hash = primaryps.getString(1).hashCode() % MAX_HASH;
//                    logger.info("hash: " + hash);
//                    if(hash < min_range)
//                        min_range = hash;
//                    if(hash > max_range)
//                        max_range = hash;
//                }
                if(min_range == Integer.MAX_VALUE)
                    min_range = 0;
                if(max_range == Integer.MIN_VALUE)
                    max_range = MAX_HASH;
                tableHashRange = min_range + "," + max_range;
                client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID + "/tables/" + tableName, tableHashRange.getBytes());
            }
        }catch(Exception e){
            e.printStackTrace();
            logger.error("Error: Master can't write table meta to zkserver");
        }
    }

    public void addTable(String name){
        try{
            client.create().withMode(CreateMode.PERSISTENT).forPath("/region" + regionID + "/tables/" + name, "0,65536".getBytes());
        }catch(Exception e){
            e.printStackTrace();
            logger.error("Error: Master can't add table information to zkserver");
        }
    }

    public void updateTable(String name, String target_regionID, String new_hash_range){
        try{
            client.setData().forPath("/region" + target_regionID + "/tables/" + name, new_hash_range.getBytes());
        }catch(Exception e){
            e.printStackTrace();
            logger.error("Error: Master can't add table information to zkserver");
        }
    }

    public boolean isTableExist(String name){
        try{
            if(client.checkExists().forPath("/region" + regionID + "/tables/" + name) != null){
                return true;
            }
        }catch(Exception e){
            logger.error("Error: Master can't check table information in zkserver");
        }
        return false;
    }

    public void removeTable(String name){
        try{
            client.delete().forPath("/region" + regionID + "/tables/" + name);
        }catch(Exception e){
            logger.error("Error: Master can't delete table information to zkserver");
        }
    }

    public void CopyFromRemoteTable(String addr, String tablename) throws SQLException {
        DatabaseConnection SourceDatabaseConnection = new DatabaseConnection("jdbc:mysql://"+ addr.substring(0, addr.indexOf(":")) +":3306/DISTRIBUTED", databaseConnection.getUsername(), databaseConnection.getPassword());
        SourceDatabaseConnection.connect();
        //清除旧表
        Connection conn = SourceDatabaseConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement("drop table " + tablename);
        ps.executeUpdate();
        TableCopy tableCopy = new TableCopy(SourceDatabaseConnection, databaseConnection,  tablename, tablename);
        tableCopy.copy();
    }

    public void CopyFromRemoteDB(String addr) throws SQLException {
        DatabaseConnection SourceDatabaseConnection = new DatabaseConnection("jdbc:mysql://"+ addr.substring(0, addr.indexOf(":")) +":3306/DISTRIBUTED", databaseConnection.getUsername(), databaseConnection.getPassword());
        SourceDatabaseConnection.connect();
        DatabaseCopy databaseCopy = new DatabaseCopy(SourceDatabaseConnection, databaseConnection);
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
            logger.error("Error: Can't get server numbers");
        }
        return 0;
    }

    public List<String> getSlaves(){
        try{
            Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
            ArrayList<String> list = new ArrayList<String>();
            for(int i = 0; i < number; i++){
                if(client.checkExists().forPath("/region" + regionID + "/slaves/slave" + i) != null){
                    list.add(new String(client.getData().forPath("/region" + regionID + "/slaves/slave" + i)));
                }
            }
            return list;
        }catch(Exception e){
            logger.error("Error: Can't get slave information");
        }
        return null;
    }


    public void close(){
        logger.info("Region server " + localaddr + " is disconneting to zkServer: "+ zkServerAddr + " ......");
        if(client != null){
            //1. 更新zk信息
            try{
                Integer number = Integer.parseInt(new String(client.getData().forPath("/region" + regionID + "/number")));
                if(number - 1 <= 0){
                    deleteAll("/region" + regionID, client);
                }
                else{
                    client.setData().forPath("/region" + regionID + "/number", Integer.valueOf(number - 1).toString().getBytes());
                    if(isMaster)
                        client.delete().forPath("/region" + regionID + "/master");
                    else
                        client.delete().forPath("/region" + regionID + "/slaves/slave" + serverID);
                }
                if(client.checkExists().forPath("/region" + regionID + "/master") == null && client.checkExists().forPath("/region" + regionID) != null &&
                   client.checkExists().forPath("/region" + regionID + "/slaves") != null){
                    List<String> children_slave = client.getChildren().forPath("/region" + regionID + "/slaves");
                    if(children_slave.size() == 0)
                        deleteAll("/region" + regionID, client);
                }
            }catch (Exception e){
                logger.warn("Error: Region Server can't delete zkserver info");
            }
            //2. 关闭client
            client.close();
            client = null;
        }
        logger.info("Disconnected successfully");
    }

    public void deleteAll(String path, CuratorFramework client) throws Exception {
        Queue<String> queue = new LinkedList<String>();
        Stack<String> deletedstack = new Stack<String>();

        queue.add(path);
        while(!queue.isEmpty()){
            String nowPath = queue.poll();
            deletedstack.push(nowPath);
            List<String> children = client.getChildren().forPath(nowPath);
            for(String child : children){
                queue.add(nowPath + "/" + child);
            }
        }
        while(!deletedstack.isEmpty()){
            String nowPath = deletedstack.pop();
            client.delete().forPath(nowPath);
        }
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
                    logger.info("Error: MasterListener can't stop listening");
                }
            }
        }


        class MyListener implements CuratorCacheListener{
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                try{
                    switch (type) {
                        case NODE_CREATED:
                            break;
                        case NODE_CHANGED:
                            break;
                        case NODE_DELETED:
                            masterUpdated(regionID, serverID, localaddr);
                            break;
                    }
                }catch(Exception e){
                    logger.info("Error: MasterListener can't handle event");
                }
            }
        }

    }

}


