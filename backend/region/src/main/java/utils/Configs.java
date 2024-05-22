package utils;

import java.util.List;

/**
 * Master服务器所需的配置信息
 */
public class Configs {
    public static String zkServer = "10.194.223.161:2181";
    public static int regionPort = 2181;
    public static int maxRegions = 2;
    public static int maxServers = 3;
    public static String DBUsername = "huang";
    public static String DBPassword = "123456"; // Exclusive
    public static String generateRegionPath(int regionId) {
        return String.format("/region%d", regionId);
    }
}