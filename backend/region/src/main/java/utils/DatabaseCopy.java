package utils;

import javax.sql.DataSource;
import java.sql.*;

/**
 * @projectName: region
 * @package: utils
 * @className: DatabaseCopy
 * @author: Huang Jinjun
 * @description: TODO
 * @date: 2024/5/7 22:12
 * @version: 1.0
 */

public class DatabaseCopy{
    private DatabaseConnection sourceDataSource;
    private DatabaseConnection targetDataSource;

    public DatabaseCopy(DatabaseConnection sourceDataSource, DatabaseConnection targetDataSource){
        this.sourceDataSource = sourceDataSource;
        this.targetDataSource = targetDataSource;
    }

    public void copy(){
        try{
            Connection sourceConnection = sourceDataSource.getConnection();
            DatabaseMetaData sourceMetaData = sourceConnection.getMetaData();
            PreparedStatement ps = sourceConnection.prepareStatement("show tables");
            ResultSet sourceTables = ps.executeQuery();

            while(sourceTables.next()){
                String sourceTable = sourceTables.getString(1);
                System.out.println("Copying table " + sourceTable + "...");
                copyTable(sourceTable, sourceTable);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Failed to copy data from source database to target database");
        }
    }
    public void copyTable(String sourceTable, String targetTable){
        TableCopy tableCopy = new TableCopy(sourceDataSource, targetDataSource, sourceTable, targetTable);
        tableCopy.copy();
    }
}
class TableCopy {
    private DatabaseConnection sourceDataSource;
    private DatabaseConnection targetDataSource;

    private final String sourceTable;
    private final String targetTable;

    public TableCopy(DatabaseConnection sourceDataSource, DatabaseConnection targetDataSource, String sourceTable, String targetTable){
        this.sourceDataSource = sourceDataSource;
        this.targetDataSource = targetDataSource;
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
    }

    public void copy(){
        try{
            Connection sourceConnection = sourceDataSource.getConnection();
            Connection targetConnection = targetDataSource.getConnection();
            //先在目标数据库创建表结构

            PreparedStatement sourceStmt = sourceConnection.prepareStatement("SELECT * FROM " + sourceTable);
            ResultSet rs = sourceStmt.executeQuery();
            ResultSetMetaData rsmeta = rs.getMetaData();

            PreparedStatement targetStmt_Create = targetConnection.prepareStatement(generateCreateStatment(sourceConnection, sourceTable));
            targetStmt_Create.executeUpdate();

            int columns = rsmeta.getColumnCount();
            while(rs.next()){
                PreparedStatement targetStmt_Insert = targetConnection.prepareStatement(generateInsertStatment(rsmeta));
                for(int i = 1; i <= columns; i++){
                    targetStmt_Insert.setObject(i, rs.getObject(i));
                }
                targetStmt_Insert.executeUpdate();
            }
        }catch (Exception e){
            if(e.getMessage().contains("already exists")) {
                System.out.println("Table " + targetTable + " already exists");
            }
            else{
                e.printStackTrace();
                System.out.println("Failed to copy data from " + sourceTable + " to " + targetTable);
            }

        }
    }

    public String generateCreateStatment(Connection conn, String Tablename) throws SQLException{
        StringBuilder sb = new StringBuilder();
        PreparedStatement stmt = conn.prepareStatement("SHOW CREATE TABLE " + Tablename);
        ResultSet rs = stmt.executeQuery();

        if(rs.next()){
            sb.append(rs.getString(2));
        }
        return sb.toString();
    }
    public String generateInsertStatment(ResultSetMetaData metaData) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(targetTable).append(" VALUES (");
        for(int i = 1; i <= metaData.getColumnCount(); i++){
            sb.append("?");
            if(i < metaData.getColumnCount()){
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
