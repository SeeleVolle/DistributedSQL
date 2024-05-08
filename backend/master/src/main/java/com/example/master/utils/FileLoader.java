package com.example.master.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.Vector;

public class FileLoader {
    private static final Logger logger = LoggerFactory.getLogger(FileLoader.class);

    private static final String resourcePath;

    public FileLoader() {
    }

    static {
        resourcePath = Objects.requireNonNull(FileLoader.class.getClassLoader().getResource("")).getPath();
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
