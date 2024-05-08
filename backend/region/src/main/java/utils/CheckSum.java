package utils;

import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
            ResultSet rs = ps.executeQuery();
            long crc = getCRC4Result(rs);
            return crc;
        }catch(SQLException e){
            System.out.println("Failed to get CRC4Table " + tableName + " in CheckSum");
        }
        return 0;
    }

    public long getCRC4Result(ResultSet rs) throws SQLException{
        int columns = rs.getMetaData().getColumnCount();
        long crc = 0;
        while(rs.next()){
            for(int i = 1; i <= columns; i++){
                System.out.println(rs.getString(i));
                crc += getCRC32(rs.getString(i));
            }
        }
        conn.close();

        return crc;
    }

    public long getCRC32(String str){
        CRC32 crc = new CRC32();
        crc.update(str.getBytes());
        return crc.getValue();
    }
}
