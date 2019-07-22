package com.panes.sample.ignite;

import java.io.Serializable;

public class DummyData implements Serializable {
    private final String key;
    private final String data;

    public DummyData(String key, String data) {
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public String getData() {
        return data;
    }
}
