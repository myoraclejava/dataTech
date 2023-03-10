package com.yunji.lakehouse.pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class F_event {
    private int id;
    private LocalDate event_date;
    private String event_type;
    private LocalDateTime event_time;
    private int uid;
    private String user_name;
    private int user_age;
    private String page;
    private LocalDateTime session_start_time;
    private LocalDateTime session_end_time;
    private int session_second;
    private int session_page_offset;
    private int session_event_offset;
    private String session_head_page;
    private String session_tail_page;
    private int session_page;

    public F_event(){}

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public LocalDate getEvent_date() {
        return event_date;
    }

    public void setEvent_date(LocalDate event_date) {
        this.event_date = event_date;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public LocalDateTime getEvent_time() {
        return event_time;
    }

    public void setEvent_time(LocalDateTime event_time) {
        this.event_time = event_time;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public int getUser_age() {
        return user_age;
    }

    public void setUser_age(int user_age) {
        this.user_age = user_age;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public LocalDateTime getSession_start_time() {
        return session_start_time;
    }

    public void setSession_start_time(LocalDateTime session_start_time) {
        this.session_start_time = session_start_time;
    }

    public LocalDateTime getSession_end_time() {
        return session_end_time;
    }

    public void setSession_end_time(LocalDateTime session_end_time) {
        this.session_end_time = session_end_time;
    }

    public int getSession_second() {
        return session_second;
    }

    public void setSession_second(int session_second) {
        this.session_second = session_second;
    }

    public int getSession_page_offset() {
        return session_page_offset;
    }

    public void setSession_page_offset(int session_page_offset) {
        this.session_page_offset = session_page_offset;
    }

    public int getSession_event_offset() {
        return session_event_offset;
    }

    public void setSession_event_offset(int session_event_offset) {
        this.session_event_offset = session_event_offset;
    }

    public String getSession_head_page() {
        return session_head_page;
    }

    public void setSession_head_page(String session_head_page) {
        this.session_head_page = session_head_page;
    }

    public String getSession_tail_page() {
        return session_tail_page;
    }

    public void setSession_tail_page(String session_tail_page) {
        this.session_tail_page = session_tail_page;
    }

    public int getSession_page() {
        return session_page;
    }

    public void setSession_page(int session_page) {
        this.session_page = session_page;
    }

}
