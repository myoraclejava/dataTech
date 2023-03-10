package com.yunji.lakehouse.pojo;

public class O_event {
    private int id;
    private long ts_ms;
    private String op;
    private String data;
    private String event_type;
    private long event_time;
    private int uid;
    private String page;
    private int event_qty;
    public O_event() {}
    public O_event(int id, long ts_ms, String op, String event_type, long event_time, int uid, String page, int event_qty, String data) {
        this.id=id;
        this.ts_ms=ts_ms;
        this.op=op;
        this.event_type=event_type;
        this.event_time=event_time;
        this.uid=uid;
        this.page=page;
        this.event_qty=event_qty;
        this.data=data;
    }

    public O_event(int id, long ts_ms, String op, String data) {
        this.id=id;
        this.ts_ms=ts_ms;
        this.op=op;
        this.data=data;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(long ts_ms) {
        this.ts_ms = ts_ms;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
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

    public int getEvent_qty() {
        return event_qty;
    }

    public void setEvent_qty(int event_qty) {
        this.event_qty = event_qty;
    }
}
