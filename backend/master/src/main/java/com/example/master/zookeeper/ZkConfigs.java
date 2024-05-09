package com.example.master.zookeeper;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;

import java.util.Vector;

public class ZkConfigs {
    public static Vector<String> zkServers;
    public static int MAX_REGION = 2;
    public static int MAX_REGION_SLAVES = 3;
    public static String generateRegionPath(int regionId) {
        return String.format("/region%d", regionId);
    }
}