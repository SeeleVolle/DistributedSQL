package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @projectName: region
 * @package: utils
 * @className: TableCOpy
 * @author: Huang Jinjun
 * @description: TODO
 * @date: 2024/5/17 10:42
 * @version: 1.0
 */
public class TableCopy {
    private static final Logger logger = LoggerFactory.getLogger(TableCopy.class);
    
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
                logger.info("Table " + targetTable + " already exists");
            }
            else{
                e.printStackTrace();
                logger.info("Failed to copy data from " + sourceTable + " to " + targetTable);
            }

        }
    }

    public String generateCreateStatment(Connection conn, String Tablename) throws SQLException {
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
