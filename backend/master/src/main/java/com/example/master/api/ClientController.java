package com.example.master.api;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientController {
    @PostMapping("/create_table")
    public ApiResult createTable() {
        return new ApiResult();
    }
}
