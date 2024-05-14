package region;

import com.alibaba.fastjson.JSONObject;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import utils.CheckSum;
import utils.DatabaseConnection;
import utils.DatabaseCopy;
import utils.Zookeeper;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    DatabaseConnection databaseConnection;

    @Value("${DatabaseConnection.username}")
    private String username;
    @Value("${DatabaseConnection.password}")
    private String password;


    private static Zookeeper zookeeper;

    public static void main(String[] args) {
        SpringApplication.run(RegionApplication.class, args);
    }

    @PostConstruct
    void init(){
        try{
            //获取本机地址：ip:port
            String ip = InetAddress.getLocalHost().getHostAddress();
            System.out.println("本机IP地址：" + ip);
            String localaddr = ip+":"+port;
//            获取数据库连接
            String url = "jdbc:mysql://"+ ip + ":3306/DISTRIBUTED";
            databaseConnection = new DatabaseConnection(url, username, password);
            databaseConnection.connect();
            //初始化zookeeper
            zookeeper = new Zookeeper(localaddr, zkServerAddr, databaseConnection, maxRegions, maxServers);
            zookeeper.connect();
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("Error: Region Server init failed.");
        }
    }

    @PreDestroy
    void destroy(){
        try{
            System.out.println("Region Server close.");
            zookeeper.close();
        } catch (Exception e){
            System.out.println("Error: Region Server close failed.");
        }
    }

    @RequestMapping("/test")
    public String test(@RequestBody String sql){
        System.out.println("SQL: " + sql);
        return "Region Server is running.";
    }



    @RequestMapping("/create")
    public JSONObject createTable(@RequestBody SQLParams params){
        System.out.println("SQL: " + params.getSql());
        JSONObject res = new JSONObject();
        //1. 执行SQL语句
        try{
            executeSQLUpdated(params.getSql());
            if(!zookeeper.isMaster()){
                CheckSum checkSum = new CheckSum(databaseConnection);
                long myCRCResult = checkSum.getCRC4Table(params.getTableName());
                long masterCRCResult = Long.valueOf(params.getCrcResult());
                if(myCRCResult != masterCRCResult){
                    String masterAddr = zookeeper.getMasterAddr();
                    zookeeper.CopyFromRemoteTable(masterAddr, params.getTableName());
                }
            }
        }catch (Exception e){
            System.out.println("Error: Region Server create table failed.");
            res.put("status", "500");
            res.put("msg", "Create table failed");
            return res;
        }
        //2. master转发sql语句到该Region下的所有slave
        if(zookeeper.isMaster()){
            while(!zookeeper.isReady());
            CheckSum checkSum = new CheckSum(databaseConnection);
            long myCRCResult = checkSum.getCRC4Table(params.getTableName());
            forward(params.getSql(), "create", params.getTableName(), myCRCResult);
        //3. 更新zk下的table信息
            try{
                zookeeper.addTable(params.getTableName());
            }catch (Exception e){
                System.out.println("Error: Region Server update table info failed.");
                res.put("status", "500");
                res.put("msg", "Update table info failed");
                return res;
            }
        }
        res.put("status", "200");
        res.put("msg", "Create table successfully");
        return res;
    }

    @RequestMapping("/drop")
    public JSONObject dropTable(@RequestBody SQLParams params){
        System.out.println("SQL: " + params.getSql());

        JSONObject res = new JSONObject();
        //1. 执行SQL语句
        try{
            executeSQLUpdated(params.getSql());
            if(!zookeeper.isMaster()){
                CheckSum checkSum = new CheckSum(databaseConnection);
                long myCRCResult = checkSum.getCRC4Table(params.getTableName());
                long masterCRCResult = Long.valueOf(params.getCrcResult());
                if(myCRCResult != masterCRCResult){
                    String masterAddr = zookeeper.getMasterAddr();
                    zookeeper.CopyFromRemoteTable(masterAddr, params.getTableName());
                }
            }
        }catch (Exception e){
            System.out.println("Error: Region Server drop table failed.");
            res.put("status", "500");
            res.put("msg", "Drop table failed");
            return res;
        }

        //2. master转发sql语句到该Region下的所有slave
        if(zookeeper.isMaster()){
            while(!zookeeper.isReady());
            CheckSum checkSum = new CheckSum(databaseConnection);
            long myCRCResult = checkSum.getCRC4Table(params.getTableName());
            forward(params.getSql(), "drop", params.getTableName(), myCRCResult);
        //3. 更新zk下的table信息
            try{
                zookeeper.removeTable(params.getTableName());
            }catch (Exception e){
                System.out.println("Error: Region Server update table info failed.");
                res.put("status", "500");
                res.put("msg", "Update table info failed");
                return res;
            }
        }
        res.put("status", "200");
        res.put("msg", "Drop table successfully");
        return res;
    }

    @RequestMapping("/query")
    public JSONObject queryTable(@RequestBody SQLParams params) {
        System.out.println("SQL: " + params.getSql());

        JSONObject res = new JSONObject();
        //1. 在本slave执行SQL语句
        try{
            ResultSet rs = executeSQLQuery(params.getSql());
            ResultSetMetaData meta = rs.getMetaData();

            List<Object[]> datalist = new ArrayList<>();
            while(rs.next()){
                Object[] rowData = new Object[rs.getMetaData().getColumnCount()];
                for(int i = 0; i < rs.getMetaData().getColumnCount(); i++){
                    rowData[i] = rs.getObject(i+1);
                }
                datalist.add(rowData);
            }

            CheckSum checkSum = new CheckSum(databaseConnection);
            long myCRCResult = checkSum.getCRC4Result(datalist);
        //2. 转发sql语句到其他slave进行vote表决
            if(vote(params.getSql(), params.getTableName(), myCRCResult)){
                res.put("status", "200");
                res.put("msg", "Query table successfully");
        //3. 返回查询结果
                int columns = meta.getColumnCount();
                StringBuilder first_row = new StringBuilder();
                for(int i = 1; i <= columns; i++){
                    if(i == columns)
                        first_row.append(meta.getColumnName(i));
                    else
                        first_row.append(meta.getColumnName(i)).append(" ");
                }
                res.put("Column Name:", first_row.toString());
                for(int i = 0; i < datalist.size(); i++){
                    StringBuilder row = new StringBuilder();
                    for(int j = 0; j < datalist.get(0).length; j++){
                        if(j == datalist.get(0).length - 1)
                            row.append(datalist.get(i)[j]);
                        else
                            row.append(datalist.get(i)[j]).append(" ");
                    }
                    res.put("Row "+ String.valueOf(i+1) +" :", row.toString());
                }
            }
            else
                throw new Exception();

        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error: Region Server query table failed.");
            res.put("status", "500");
            res.put("message", "Query table failed.");
        }
        return res;
    }

    @RequestMapping("/update")
    public JSONObject updateTable(@RequestBody SQLParams params){
        System.out.println("SQL: " + params.getSql());

        JSONObject res = new JSONObject();
        //1. 执行SQL语句
        try{
            executeSQLUpdated(params.getSql());
            if(!zookeeper.isMaster()){
                CheckSum checkSum = new CheckSum(databaseConnection);
                long myCRCResult = checkSum.getCRC4Table(params.getTableName());
                long masterCRCResult = Long.valueOf(params.getCrcResult());
                if(myCRCResult != masterCRCResult){
                    String masterAddr = zookeeper.getMasterAddr();
                    zookeeper.CopyFromRemoteTable(masterAddr, params.getTableName());
                }
            }
        }catch (Exception e){
            System.out.println("Error: Region Server update table failed.");
            res.put("status", "500");
            res.put("msg", "Update table failed");
            return res;
        }

        //2. master转发sql语句到该Region下的所有slave
        if(zookeeper.isMaster()){
            while(!zookeeper.isReady());
            CheckSum checkSum = new CheckSum(databaseConnection);
            long myCRCResult = checkSum.getCRC4Table(params.getTableName());
            forward(params.getSql(), "update", params.getTableName(), myCRCResult);
        }
        res.put("status", "200");
        res.put("msg", "Update table successfully");
        return res;
    }

    public void executeSQLUpdated(String sql){
        try{
            Connection conn = databaseConnection.getConnection();
            System.out.println("SQL Executed: " + sql);
            //SQL注入风险，不管了。。
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error: Region Server execute sql failed.");
        }
    }

    public ResultSet executeSQLQuery(String sql){
        try{
            Connection conn = databaseConnection.getConnection();
            System.out.println("SQL Executed: " + sql);
            //SQL注入风险，不管了。。
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            return rs;
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error: Region Server execute sql failed.");
        }
        return null;
    }

    public void forward(String sql,  String type, String tableName, long myCRCResult){
        List<String> slavesAddrs = zookeeper.getSlaves();
        for(String slaveAddr: slavesAddrs) {
            String slaveurl = "http://" + slaveAddr.substring(0, slaveAddr.indexOf(":")) + ":9090/" + type;
            RestTemplate restTemplate = new RestTemplate();
            //设置参数
            JSONObject params = new JSONObject();
            params.put("sql", sql);
            params.put("tableName", tableName);
            params.put("CRCResult", String.valueOf(myCRCResult));
            //设置请求头和请求体
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<JSONObject> requestEntity = new HttpEntity<>(params, headers);
            //发送请求并获取响应
            ResponseEntity<String> responseEntity = restTemplate.exchange(slaveurl, HttpMethod.POST, requestEntity, String.class);
            int statusCode = responseEntity.getStatusCode().value();
            if (statusCode == 200) {
                System.out.println("Forward to " + slaveAddr + " successfully.");
            } else {
                System.out.println("Forward to " + slaveAddr + " failed.");
            }
        }
    }

    public boolean vote(String sql, String tableName, long myCRCResult) throws SQLException {
        List<String> slavesAddrs = zookeeper.getSlaves();
        int supports_num = 1;
        //转发请求并获取结果
        for(String slaveAddr: slavesAddrs) {
            String slaveurl = "http://" + slaveAddr.substring(0, slaveAddr.indexOf(":")) + ":9090/" + "votequery";
            RestTemplate restTemplate = new RestTemplate();
            //设置参数
            JSONObject params = new JSONObject();
            params.put("sql", sql);
            params.put("tableName", tableName);
            //设置请求头和请求体
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<JSONObject> requestEntity = new HttpEntity<>(params, headers);
            //发送请求并获取响应
            String CRCResult = restTemplate.postForEntity(slaveurl, requestEntity, JSONObject.class).getBody().get("CRCResult").toString();
            if(CRCResult.equals(myCRCResult)) {
                supports_num++;
            }
        }
        if(supports_num >= slavesAddrs.size() / 2  + 1 ) {
            return true;
        }
        System.out.println("Error: Vote can't exceed half of the slaves.");
        return false;
    }

    @RequestMapping("/votequery")
    public JSONObject  votequery(@RequestBody SQLParams params){
        JSONObject res = new JSONObject();
        ResultSet rs = executeSQLQuery(params.getSql());
        CheckSum checkSum = new CheckSum(databaseConnection);
        try{
            res.put("CRCResult", checkSum.getCRC4ResultSet(rs));
        }catch(Exception e){
            res.put("status",  "500");
            System.out.println("Error: Get CRC4Result failed.");
            return res;
        }
        res.put("status", "200");
        return res;
    }
}

class SQLParams{
    private String sql;
    private String tableName;
    private long CRCResult;

    SQLParams(String sql, String tableName, long CRCResult){
        this.sql = sql;
        this.tableName = tableName;
        this.CRCResult = CRCResult;

        this.tableName = this.tableName.toUpperCase();
        this.sql = this.sql.replaceFirst(tableName, this.tableName);
    }

    public String getSql() {
        return sql;
    }

    public String getTableName() {
        return tableName;
    }

    public long getCrcResult() { return CRCResult; }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setCrcResult(long CRCResult) { this.CRCResult = CRCResult; }
}

