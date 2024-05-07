package com.example.master.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.Vector;

public class StaticResourcesLoader {
    private static final Logger logger = LoggerFactory.getLogger(StaticResourcesLoader.class);

    private static final String resourcePath;

    public StaticResourcesLoader() {
    }

    static {
        resourcePath = Objects.requireNonNull(StaticResourcesLoader.class.getClassLoader().getResource("")).getPath();
    }

    public static Vector<String> loadZkServers() {
        Vector<String> zkServers = new Vector<>();
        try {
            List<String> lines = Files.readAllLines(new File(resourcePath + "zkServers.mini").toPath());
            zkServers.addAll(lines);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return zkServers;
    }
}
