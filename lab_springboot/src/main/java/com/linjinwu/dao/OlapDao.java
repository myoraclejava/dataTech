package com.linjinwu.dao;

import com.linjinwu.pojo.OlapColumn;
import com.linjinwu.pojo.OlapResultSet;

public interface OlapDao {
    public String produceSql(OlapColumn[] olapQueries, String dimTableName, String[] dimColName);
    public OlapResultSet queryData(OlapColumn[] olapColumns);
    public OlapResultSet queryData(OlapColumn[] olapColumns, int pageNum, int pageSize);

}
