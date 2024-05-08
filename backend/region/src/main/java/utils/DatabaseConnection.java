package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @projectName: region
 * @package: utils
 * @className: DatabaseConnection
 * @author: Huang Jinjun
 * @description: TODO
 * @date: 2024/5/8 16:34
 * @version: 1.0
 */
public class DatabaseConnection {
    private final String url;
    private final String username;
    private final String password;
    private Connection connection;

    public DatabaseConnection(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public void connect() throws SQLException {
        connection = DriverManager.getConnection(url, username, password);
    }

    public Connection getConnection() {
        return connection;
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}
