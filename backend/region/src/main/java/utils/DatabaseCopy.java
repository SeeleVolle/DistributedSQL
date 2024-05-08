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
            ResultSet sourceTables = sourceMetaData.getTables(null, null, null, new String[]{"TABLE"});
            while(sourceTables.next()){
                String sourceTable = sourceTables.getString("TABLE_NAME");
                copyTable(sourceTable, sourceTable);
            }
        }catch (Exception e){
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

            PreparedStatement targetStmt_Create = targetConnection.prepareStatement(generateInsertStatment(rsmeta));
            targetStmt_Create.executeUpdate();

            int columns = rsmeta.getColumnCount();
            PreparedStatement targetStmt_Insert = targetConnection.prepareStatement(generateInsertStatment(rsmeta));
            while(rs.next()){
                for(int i = 1; i <= columns; i++){
                    targetStmt_Insert.setObject(i, rs.getObject(i));
                }
                targetStmt_Insert.executeUpdate();
            }
        }catch (Exception e){
            System.out.println("Failed to copy data from " + sourceTable + " to " + targetTable);
        }
    }

    public String generateCreateStatment(ResultSetMetaData metaData) throws SQLException{
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(targetTable).append(" (");
        for(int i = 1; i <= metaData.getColumnCount(); i++){
            sb.append(metaData.getColumnName(i)).append(" ").append(metaData.getColumnTypeName(i));
            if(i < metaData.getColumnCount()){
                sb.append(",");
            }

        }
        sb.append(")");
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
