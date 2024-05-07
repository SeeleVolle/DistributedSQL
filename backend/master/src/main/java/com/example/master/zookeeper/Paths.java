package com.example.master.zookeeper;

import lombok.Getter;

@Getter
enum Paths {
    MASTER("/master"),
    SLAVE("/slave");

    private final String value;

    Paths(String value) {
        this.value = value;
    }
}
