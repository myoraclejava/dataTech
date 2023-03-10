package com.yunji.lakehouse.pojo;

public class Event {
    private int id;
    private String event_type;
    private long event_time;
    private int uid;
    private String page;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getEvent_time() {
        return event_time;
    }

    public void setEvent_time(long event_time) {
        this.event_time = event_time;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }
}
