package com.example.master.api;

import com.alibaba.fastjson2.JSONObject;
import com.example.master.api.pojo.Data;
import com.example.master.zookeeper.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.example.master.api.ApiResultCode.*;

@RestController
public class ClientController {
    private static final Logger logger = LoggerFactory.getLogger(ClientController.class);
    private final Metadata metadata = Metadata.getInstance();

    @GetMapping("/")
    public ApiResult hello() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return new ApiResult().success().message("Hello, this is master server");
    }

    @GetMapping("/test")
    public ApiResult test() {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(()-> {
            RestTemplate restTemplate = new RestTemplate();
            String result = restTemplate.getForObject("http://localhost:8081/", String.class);
            return result;
        });
        try {
            String r = future.get();
            System.out.println(r);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        return new ApiResult().success().message("");
    }

    @PostMapping("/create_table")
    public ApiResult createTable(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request create table '{}'", tableName);
        Data data = new Data();
        if (metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_EXIST.getCode(), TABLE_EXIST.getMessage(), data);
        } else if (!metadata.hasWritable()) {
            return new ApiResult().failed().message("No writable region server, table creation failed.").data(data);
        } else {
            data.setHostName(metadata.pickServer(tableName, Metadata.OperationType.CREATE_TABLE));
            logger.info("Table '{}' is created on '{}'", tableName, data.getHostName());
            return new ApiResult().success().data(data);
        }
    }

    @PostMapping("/query_table")
    public ApiResult queryTable(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request query table '{}'", tableName);
        Data data = new Data();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getCode(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.setHostName(metadata.pickServer(tableName, Metadata.OperationType.QUERY_TABLE));
            return new ApiResult().success().data(data);
        }
    }

    @PostMapping("/insert_table")
    public ApiResult insertTable() {
        return new ApiResult().failed().message("Haven't implemented yet").data(new Data());
    }

    @PostMapping("/update_table")
    public ApiResult updateTable() {
        return new ApiResult().failed().message("Haven't implemented yet").data(new Data());
    }

    @PostMapping("/drop_table")
    public ApiResult dropTable() {
        return new ApiResult().failed().message("Haven't implemented yet").data(new Data());
    }

    @PostMapping("/write_table")
    public ApiResult writeTable(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request write table '{}'", tableName);
        Data data = new Data();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getCode(), TABLE_NOT_EXIST.getMessage(), data);
        } else if (!metadata.hasWritable()) {
            return new ApiResult().failed().message("No writable region server, table create failed.");
        } else {
            data.setHostName(metadata.pickServer(tableName, Metadata.OperationType.WRITE_TABLE));
            return new ApiResult().success().data(data);
        }
    }

    @PostMapping("/meta_info")
    public ApiResult metaInfo() {
        logger.info("Getting all regions' metadata information");
        JSONObject data = new JSONObject();
        data.put("meta", metadata);
        return new ApiResult().success().data(data);
    }
}
