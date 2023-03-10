package com.linjinwu.pojo;

import java.util.List;
import java.util.Map;

public class OlapResultSet {

    private int pageNum = 0;
    private int pageSize = 0;
    private int rowQty = 0;
    private int totalRowQty = 0;
    private List<Map> rowSet;

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getRowQty() {
        return rowQty;
    }

    public void setRowQty(int rowQty) {
        this.rowQty = rowQty;
    }

    public int getTotalRowQty() {
        return totalRowQty;
    }

    public void setTotalRowQty(int totalRowQty) {
        this.totalRowQty = totalRowQty;
    }

    public List<Map> getRowSet() {
        return rowSet;
    }

    public void setRowSet(List<Map> rowSet) {
        this.rowSet = rowSet;
    }

}
