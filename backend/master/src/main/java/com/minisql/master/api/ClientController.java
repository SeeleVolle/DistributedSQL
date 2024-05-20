package com.minisql.master.api;

import com.alibaba.fastjson2.JSONObject;
import com.minisql.master.zookeeper.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.minisql.master.api.StatusCode.*;

/**
 * 这是一个接受来自Client请求的Controller
 */
@RestController
public class ClientController {
    private static final Logger logger = LoggerFactory.getLogger(ClientController.class);
    private final Metadata metadata = Metadata.getInstance();

    /**
     * 测试用的接口
     *
     * @param tables Dummy data
     * @return 返回
     */
    @GetMapping("/")
    public ApiResult hello(@RequestBody List<Metadata.RegionMetadata.Table> tables) {
        return new ApiResult().ok().message("Hello, this is master server").data(tables);
    }

    /**
     * 这个接口会透过数据库的元数据选择一个合适的Region来进行建表操作，会返回Region对应的Master
     *
     * @param tableName 所建的表名
     * @return 返回负责建表的master
     */
    @PostMapping("/create_table")
    public ApiResult createTable(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request create table '{}'", tableName);
        JSONObject data = new JSONObject();
        if (metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_EXIST.getStatus(), TABLE_EXIST.getMessage(), data);
        } else if (!metadata.hasWritable()) {
            return new ApiResult().failed().message("No writable region server, table creation failed.").data(data);
        } else {
            data.put("hostNames", metadata.pickServer(tableName, Metadata.OperationType.CREATE_TABLE, ""));
            logger.info("Table '{}' is created on '{}'", tableName, data.get("hostNames"));
            return new ApiResult().ok().data(data).message("OK，返回负责建表的Region Master");
        }
    }

    /**
     *
     * @param tableName 要查询的表名
     * @return 返回负责处理查询的slave(s)
     */
    @PostMapping("/query_table")
    public ApiResult queryTable(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request query table '{}'", tableName);
        JSONObject data = new JSONObject();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getStatus(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.put("hostNames", metadata.pickServer(tableName, Metadata.OperationType.QUERY_TABLE, ""));
            return new ApiResult().ok().data(data).message("OK，返回所有负责处理查询操作的Slaves");
        }
    }

    /**
     *
     * @param tableName 要插入到的表
     * @param pkValue 主键值
     * @return 返回负责处理插入表的master
     */
    @PostMapping("/insert")
    public ApiResult insert(String tableName, String pkValue) {
        logger.info("Request insert record into table '{}'", tableName);
        JSONObject data = new JSONObject();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getStatus(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.put("hostNames", metadata.pickServer(tableName, Metadata.OperationType.INSERT_TABLE, pkValue));
            return new ApiResult().ok().data(data).message("OK，返回一个负责处理插入记录操作的Region Master");
        }
    }

    /**
     *
     * @param tableName 要修改的表名
     * @return 返回负责处理修改表的master(s)
     */
    @PostMapping("/update")
    public ApiResult update(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request update table '{}'", tableName);
        JSONObject data = new JSONObject();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getStatus(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.put("hostNames", metadata.pickServer(tableName, Metadata.OperationType.UPDATE_TABLE, ""));
            return new ApiResult().ok().data(data).message("OK，返回所有负责处理更新记录操作的Region Master");
        }
    }

    /**
     *
     * @param tableName 要删除的记录所对应的表
     * @return 返回负责删除记录的master(s)
     */
    @PostMapping("/delete")
    public ApiResult delete(@RequestParam(name = "tableName") String tableName) {
        logger.info("Request delete record from table '{}'", tableName);
        JSONObject data = new JSONObject();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getStatus(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.put("hostNames", metadata.pickServer(tableName, Metadata.OperationType.DELETE_TABLE, ""));
            return new ApiResult().ok().data(data).message("OK，返回所有负责处理删除记录操作的Region Master");
        }
    }

    /**
     *
     * @param tableName 要删除的表名
     * @return 返回负责处理删表的master(s)
     */
    @PostMapping("/drop_table")
    public ApiResult dropTable(String tableName) {
        logger.info("Request drop table '{}'", tableName);
        JSONObject data = new JSONObject();
        if (!metadata.hasTable(tableName)) {
            return new ApiResult(TABLE_NOT_EXIST.getStatus(), TABLE_NOT_EXIST.getMessage(), data);
        } else {
            data.put("hostNames", metadata.pickServer(tableName, Metadata.OperationType.DROP_TABLE, ""));
            return new ApiResult().ok().data(data).message("OK，返回所有负责处理删除表操作的Region Master");
        }
    }

    /**
     *
     * @return 返回整个数据库当前状态的元数据
     */
    @PostMapping("/meta_info")
    public ApiResult metaInfo() {
        logger.info("Getting all regions' metadata information");
        JSONObject data = new JSONObject();
        data.put("meta", metadata);
        return new ApiResult().ok().data(data);
    }
}
