package com.yunji.lakehouse.pojo;

import java.util.HashMap;
import java.util.Optional;

public class UserDailyPv {
    private long startTime = 0;
    private long endTime = 0;
    private String uid = "";
    private String op = "";
    private int pv = 1;
    private long statTime = 0;
    private String eventTime = "1970-01-01 00:00:00";
    private String statRange = "";
    private long driverTime = 0;

    public UserDailyPv(long startTime, long endTime, String uid, int pv, long statTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.uid = uid;
        this.pv = pv;
        this.statTime = statTime;
    }

    public UserDailyPv(HashMap<String,Object> m) {
        this.startTime = (long) Optional.ofNullable(m.get("startTime")).orElse(this.startTime);
        this.endTime = (long) Optional.ofNullable(m.get("endTime")).orElse(this.endTime);
        this.uid = (String) Optional.ofNullable(m.get("uid")).orElse(this.uid);
        this.pv = (Integer) Optional.ofNullable(m.get("pv")).orElse(this.pv);
        this.statTime = (long) Optional.ofNullable(m.get("statTime")).orElse(this.statTime);
        this.statRange = (String) Optional.ofNullable(m.get("statRange")).orElse(this.statRange);
        this.eventTime = (String) Optional.ofNullable(m.get("eventTime")).orElse(this.eventTime);
        this.op = (String) Optional.ofNullable(m.get("op")).orElse(this.op);
        this.driverTime = (long) Optional.ofNullable(m.get("driverTime")).orElse(this.driverTime);
    }

    public UserDailyPv(String uid, String eventTime) {
        this.uid = uid;
        this.eventTime = eventTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getUid() {
        return uid;
    }

    public int getPv() {
        return pv;
    }

    public long getStatTime() {
        return statTime;
    }

    public String getEventTime() {
        return eventTime;
    }

    public UserDailyPv setEventTime(String eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    public UserDailyPv setPv(int pv) {
        this.pv = pv;
        return this;
    }

    public UserDailyPv setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public UserDailyPv setEndTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    public UserDailyPv setStatTime(long statTime) {
        this.statTime = statTime;
        return this;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getStatRange() {
        return statRange;
    }

    public void setStatRange(String statRange) {
        this.statRange = statRange;
    }

    public long getDriverTime() {
        return driverTime;
    }

    public void setDriverTime(long driverTime) {
        this.driverTime = driverTime;
    }
}
