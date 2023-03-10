package com.linjinwu.pojo;

public class OlapColumn {
    private String queryName;
    private String tableName;
    private String whereClause = "";
    private String[] groupByColNames;
    private String aggColName;

    private String aggOp = "";

    private int aggColDataType = 0;
    private int sortTag = 0;

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public String[] getGroupByColNames() {
        return groupByColNames;
    }

    public void setGroupByColNames(String[] groupByColNames) {
        this.groupByColNames = groupByColNames;
    }

    public String getAggColName() {
        return aggColName;
    }

    public void setAggColName(String aggColName) {
        this.aggColName = aggColName;
    }

    public String getAggOp() { return aggOp; }

    public void setAggOp(String aggOp) { this.aggOp = aggOp; }

    public int getAggColDataType() { return aggColDataType; }

    public void setAggColDataType(int aggColDataType) { this.aggColDataType = aggColDataType; }

    public int getSortTag() {
        return sortTag;
    }

    public void setSortTag(int sortTag) {
        this.sortTag = sortTag;
    }

}
