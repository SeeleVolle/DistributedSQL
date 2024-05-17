package com.example.master.zookeeper;

import java.util.List;
import java.util.Vector;

public class ZkConfigs {
    public static List<String> zkServers;
    public static int MAX_REGION = 2;
    public static int HOTPOINT_THRESHOLD = 1;
    public static int MAX_HASH = 65536; // Exclusive

    public static String generateRegionPath(int regionId) {
        return String.format("/region%d", regionId);
    }
}