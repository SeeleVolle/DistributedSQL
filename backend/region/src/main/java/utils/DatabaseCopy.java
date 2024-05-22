package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(DatabaseCopy.class);
    
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
                logger.info("Copying table " + sourceTable + "...");
                copyTable(sourceTable, sourceTable);
            }
        }catch (Exception e){
            e.printStackTrace();
            logger.error("Failed to copy data from source database to target database");
        }
    }
    public void copyTable(String sourceTable, String targetTable){
        TableCopy tableCopy = new TableCopy(sourceDataSource, targetDataSource, sourceTable, targetTable);
        tableCopy.copy();
    }
}
