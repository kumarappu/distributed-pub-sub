package com.ak.pubsub.poc.pojo;

import java.io.Serializable;

/**
 * Created by appu_kumar on 4/23/2019.
 */
public class Ticker implements Serializable {

    private static final long serialVersionUID = 1L;

    public Ticker() {
    }

    private String name;
    private  long time;

    public Ticker(String name, long time) {
        this.name = name;
        this.time = time;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Ticker{" +
                "name='" + name + '\'' +
                ", time=" + time +
                '}';
    }
}
