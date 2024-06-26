package com.minisql.master.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * 负责Master服务器数据持久化相关的工具类
 */
public class PersistenceHandler {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceHandler.class);

    public PersistenceHandler() {
    }

    public static void loadConfigurations(String file) {
        try {
            logger.info("Loading configuration file '{}'", file);
            byte[] data = Files.readAllBytes(new File(file).toPath());
            JSONObject jsonObject;
            try {
                jsonObject = JSON.parseObject(new String(data));
            } catch (Exception e) {
                logger.error("Failed to parse configurations file");
                return;
            }
            Configs.ZK_SERVERS = jsonObject.getList("ZK_SERVERS", String.class);
            Configs.MAX_HOTPOINT_THRESHOLD = jsonObject.getInteger("MAX_HOTPOINT_THRESHOLD");
            Configs.MIN_HOTPOINT_THRESHOLD = jsonObject.getInteger("MIN_HOTPOINT_THRESHOLD");
            Configs.MAX_REGION = jsonObject.getInteger("MAX_REGION");
            Configs.MAX_HASH = jsonObject.getInteger("MAX_HASH");
            Configs.REGION_SERVER_PORT = jsonObject.getInteger("REGION_SERVER_PORT");
            if (jsonObject.getBoolean("REGION_SERVER_HTTPS")) {
                Configs.REGION_SERVER_HTTPS = "https";
            } else {
                Configs.REGION_SERVER_HTTPS = "http";
            }
            logger.info("Configurations loaded...");
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }
}
