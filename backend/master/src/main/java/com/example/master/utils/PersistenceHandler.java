package com.example.master.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
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
            Configs.ZK_SERVERS = jsonObject.getList("ZK_SERVERS", String.class);
            Configs.HOTPOINT_THRESHOLD = jsonObject.getInteger("HOTPOINT_THRESHOLD");
            Configs.MAX_REGION = jsonObject.getInteger("MAX_REGION");
            Configs.MAX_HASH = jsonObject.getInteger("MAX_HASH");
            Configs.REGION_SERVER_PORT = jsonObject.getInteger("REGION_SERVER_PORT");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
