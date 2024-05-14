package utils;

import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.zip.CRC32;

/**
 * @projectName: region
 * @package: utils
 * @className: CheckSum
 * @author: Huang Jinjun
 * @description: 数据库表和查询结果的CRC32校验和
 * @date: 2024/5/7 21:20
 * @version: 1.0
 */

public class CheckSum extends CRC32 {
    DatabaseConnection databaseConnection;
    Connection conn;

    public CheckSum(DatabaseConnection databaseConnection) {
        this.databaseConnection = databaseConnection;
        try{
            this.conn = databaseConnection.getConnection();
        } catch(Exception e){
            System.out.println("Failed to connect to database in CheckSum");
        }
    }

    public long getCRC4Table(String tableName){
        try{
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tablers = metaData.getTables(null, null, tableName, null);
            if(tablers.next()){
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
                ResultSet rs = ps.executeQuery();
                long crc = getCRC4ResultSet(rs);
                return crc;
            }
        }catch(SQLException e){
            System.out.println("Failed to get CRC4Table " + tableName + " in CheckSum");
        }
        //不存在就返回0
        return 0;
    }

    public long getCRC4Result(List<Object[]> datalist) throws SQLException{
        int columns =  datalist.get(0).length;
        long crc = 0;
        for(int i = 0; i < datalist.size(); i++){
            for(int j = 0; j < columns; j++){
                crc += getCRC32(datalist.get(i)[j].toString());
            }
        }
        return crc;
    }

    public long getCRC4ResultSet(ResultSet rs) throws SQLException{
        int columns = rs.getMetaData().getColumnCount();
        long crc = 0;
        while(rs.next()){
            for(int i = 1; i <= columns; i++){
                crc += getCRC32(rs.getString(i));
            }
        }
        return crc;
    }
    public long getCRC32(String str){
        CRC32 crc = new CRC32();
        crc.update(str.getBytes());
        return crc.getValue();
    }
}
