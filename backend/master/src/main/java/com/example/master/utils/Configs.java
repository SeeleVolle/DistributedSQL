package com.example.master.utils;

import java.util.List;

/**
 * Master服务器所需的配置信息
 */
public class Configs {
    public static List<String> ZK_SERVERS;
    public static int MAX_REGION = 2;
    public static int HOTPOINT_THRESHOLD = 1;
    public static int MAX_HASH = 65536; // Exclusive
    public static int REGION_SERVER_PORT= 9090;
    public static String REGION_SERVER_HTTPS = "http";

    public static String generateRegionPath(int regionId) {
        return String.format("/region%d", regionId);
    }
}