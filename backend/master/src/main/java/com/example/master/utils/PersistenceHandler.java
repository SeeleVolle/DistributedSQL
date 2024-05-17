package com.example.master.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Vector;

public class PersistenceHandler {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceHandler.class);

    public PersistenceHandler() {
    }

    public static Vector<String> loadZkServers(String file) {
        Vector<String> zkServers = new Vector<>();
        try {
            List<String> lines = Files.readAllLines(new File(file).toPath());
            zkServers.addAll(lines);
        } catch (IOException e) {
            logger.error(e.getMessage());
            String defaultHostName = "localhost:2181";
            zkServers.add(defaultHostName);
            logger.warn("Default zkServer hostname is set: {}", defaultHostName);
        }
        return zkServers;
    }

}
