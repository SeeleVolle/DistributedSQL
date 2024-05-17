package com.example.master.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.example.master.zookeeper.ZkConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class PersistenceHandler {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceHandler.class);

    public PersistenceHandler() {
    }

    public static void loadConfigurations(String file) {
        try {
            byte[] data = Files.readAllBytes(new File(file).toPath());
            JSONObject jsonObject = JSON.parseObject(new String(data));
            ZkConfigs.zkServers = jsonObject.getList("zkServers", String.class);
            ZkConfigs.HOTPOINT_THRESHOLD = jsonObject.getInteger("HOTPOINT_THRESHOLD");
            ZkConfigs.MAX_REGION = jsonObject.getInteger("MAX_REGION");
            ZkConfigs.MAX_HASH = jsonObject.getInteger("MAX_HASH");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
