package com.example.master.zookeeper;

import lombok.Getter;

@Getter
enum Paths {
    MASTER("/master"),
    SLAVE("/slaves"),
    TABLE("/tables");

    private final String path;

    Paths(String path) {
        this.path = path;
    }
}
