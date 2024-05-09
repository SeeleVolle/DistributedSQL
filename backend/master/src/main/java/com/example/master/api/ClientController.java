package com.example.master.api;

import com.alibaba.fastjson2.JSONObject;
import com.example.master.api.pojo.Data;
import com.example.master.zookeeper.Metadata;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.example.master.api.ApiResultCode.*;

@RestController
public class ClientController {
    private final Metadata metadata = Metadata.getInstance();

    @GetMapping("/")
    public ApiResult hello() {
        return new ApiResult().success().message("Hello, this is master server");
    }

    @PostMapping("/create_table")
    public ApiResult createTable(@RequestParam String tableName) {
        Data data = new Data();
        if (metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_EXIST.getCode(), TABLE_EXIST.getMessage(), data);
        } else if (!metadata.hasWritable()) {
            return new ApiResult(INTERNAL_SERVER_ERROR.getCode(), INTERNAL_SERVER_ERROR.getMessage(), data);
        } else {
            data.setHostName(metadata.pickServer(tableName, Metadata.OperationType.CREATE_TABLE));
            return new ApiResult().success().data(data);
        }
    }

    @PostMapping("/query_table")
    public ApiResult queryTable(@RequestParam String tableName) {
        Data data = new Data();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getCode(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.setHostName(metadata.pickServer(tableName, Metadata.OperationType.QUERY_TABLE));
            return new ApiResult().success().data(data);
        }
    }

    @PostMapping("/write_table")
    public ApiResult writeTable(@RequestParam String tableName) {
        Data data = new Data();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getCode(), TABLE_NOT_EXIST.getMessage(), data);
        } else if (!metadata.hasWritable()) {
            return new ApiResult().failed().message("No region is writable");
        } else {
            data.setHostName(metadata.pickServer(tableName, Metadata.OperationType.WRITE_TABLE));
            return new ApiResult().success().data(data);
        }
    }

    @PostMapping("/meta_info")
    public ApiResult metaInfo() {
        JSONObject data = new JSONObject();
        data.put("meta", metadata);
        return new ApiResult().success().data(data);
    }
}
