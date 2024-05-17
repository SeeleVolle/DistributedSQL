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
