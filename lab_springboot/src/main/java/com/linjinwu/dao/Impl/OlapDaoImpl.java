package com.linjinwu.dao.Impl;

import com.linjinwu.dao.OlapDao;
import com.linjinwu.pojo.OlapColumn;
import com.linjinwu.pojo.OlapResultSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.*;

import static java.lang.Math.abs;

@Repository
public class OlapDaoImpl implements OlapDao {
    @Autowired
    @Qualifier("postgresqlJdbcTemplate")
    private JdbcTemplate postgresqlJdbcTemplate;

    public String produceSql(OlapColumn[] olapColumns, String dimTableName, String[] dimColName){
        String lineSepr = System.lineSeparator();
        StringBuilder withSqlClause = new StringBuilder("with ");
        Map<Integer,StringBuffer> coalesceCols = new HashMap<>();
        StringBuilder coalesceClause = new StringBuilder();
        StringBuilder aggClause = new StringBuilder();
        StringBuilder fromClause = new StringBuilder("from ");
        StringBuilder joinClause = new StringBuilder();
        Map<Integer,String> orderByCols = new HashMap<>();
        StringBuilder orderByClause = new StringBuilder("order by ");
        String subQueryName;
        String fromQueryName = "";
        String subQueryColName;
        String[] subQueryGroupByColNames;
        String[] fromQueryGroupByColNames;

        fromQueryName = olapColumns[0].getQueryName();
        fromQueryGroupByColNames = olapColumns[0].getGroupByColNames();
        fromClause.append(fromQueryName);

        for(int i=0;i<olapColumns.length;i++) {
            subQueryName = olapColumns[i].getQueryName();
            subQueryGroupByColNames = olapColumns[i].getGroupByColNames();
            withSqlClause.append(lineSepr).append("  ").append(subQueryName).append(" as (select ");
            if(i>0){
                joinClause.append(lineSepr+"left join ").append(subQueryName).append(" on ");
            }

            for(int z=0;z<subQueryGroupByColNames.length;z++){
                withSqlClause.append(subQueryGroupByColNames[z]).append(",");
                if(i==0)
                    coalesceCols.put(z,new StringBuffer("coalesce("));
                coalesceCols.get(z).append(subQueryName).append(".").append(subQueryGroupByColNames[z]).append(",");
                if(i>0)
                    joinClause.append(subQueryName).append(".").append(subQueryGroupByColNames[z]).append("=")
                            .append(fromQueryName).append(".").append(fromQueryGroupByColNames[z]).append(" and ");
            }
            if(i>0)
                joinClause.delete(joinClause.length()-5,joinClause.length()-1);

            if(olapColumns[i].getAggOp() != null){
                subQueryColName = olapColumns[i].getAggColName() + "_" + olapColumns[i].getAggOp();
                String defaultValueClause = "''";
                switch (olapColumns[i].getAggColDataType()){
                    case 0:
                        defaultValueClause = "0";
                        break;
                    case 1:
                        defaultValueClause = "0.0";
                        break;
                    case 2:
                        defaultValueClause = "";
                        break;
                    case 3:
                        defaultValueClause = "";
                        break;
                }
                switch (olapColumns[i].getAggOp()){
                    case "countDist":
                        withSqlClause.append("count(distinct ");
                        aggClause.append("coalesce(cast(").append(subQueryName).append(".").append(subQueryColName).append(" as varchar),'").append(defaultValueClause).append("') as ").append(subQueryName).append(",");
                        break;
                    case "count":
                        withSqlClause.append("count(");
                        aggClause.append("coalesce(cast(").append(subQueryName).append(".").append(subQueryColName).append(" as varchar),'").append(defaultValueClause).append("') as ").append(subQueryName).append(",");
                        break;
                    case "sum":
                        withSqlClause.append("sum(");
                        aggClause.append("coalesce(cast(").append(subQueryName).append(".").append(subQueryColName).append(" as varchar),'").append(defaultValueClause).append("') as ").append(subQueryName).append(",");
                        break;
                    case "min":
                        withSqlClause.append("min(");
                        aggClause.append("coalesce(").append(olapColumns[i].getAggColDataType()==3?"to_char(":"cast(").append(subQueryName).append(".").append(subQueryColName)
                                .append(olapColumns[i].getAggColDataType()==3?",'yyyy-mm-dd')":" as varchar)").append(",'").append(defaultValueClause).append("') as ").append(subQueryName).append(",");
                        break;
                    case "max":
                        withSqlClause.append("max(");
                        aggClause.append("coalesce(").append(olapColumns[i].getAggColDataType()==3?"to_char(":"cast(").append(subQueryName).append(".").append(subQueryColName)
                                .append(olapColumns[i].getAggColDataType()==3?",'yyyy-mm-dd')":" as varchar)").append(",'").append(defaultValueClause).append("') as ").append(subQueryName).append(",");
                        break;
                }

                withSqlClause.append(olapColumns[i].getAggColName()).append(")").append(" as ").append(subQueryColName);
                if(olapColumns[i].getSortTag() != 0)
                    orderByCols.put(abs(olapColumns[i].getSortTag()),
                            olapColumns[i].getSortTag()>0?(subQueryName+" asc"):(subQueryName+" desc"));
            }

            withSqlClause.append(" from ").append(olapColumns[i].getTableName());

            if(!"".equals(olapColumns[i].getWhereClause()))
                    withSqlClause.append(" where ").append(olapColumns[i].getWhereClause());

            withSqlClause.append(" group by ");
            for(String subQueryGroupByColName:subQueryGroupByColNames)
                withSqlClause.append(subQueryGroupByColName).append(",");
            withSqlClause.deleteCharAt(withSqlClause.length()-1).append("),");
        }

        for(int k=1;k<=orderByCols.size();k++)
            orderByClause.append(orderByCols.get(k)).append(",");

        aggClause.deleteCharAt(aggClause.length()-1);
        withSqlClause.deleteCharAt(withSqlClause.length()-1);

        for(int j=0;j<fromQueryGroupByColNames.length;j++) {
            coalesceCols.get(j).deleteCharAt(coalesceCols.get(j).length()-1);
            coalesceCols.get(j).append(") as ").append(fromQueryGroupByColNames[j]);
            //orderByClause.append(fromQueryGroupByColNames[j]).append(",");
        }
        for(Map.Entry coalesceCol : coalesceCols.entrySet())
            coalesceClause.append(coalesceCol.getValue()).append(",");
        orderByClause.deleteCharAt(orderByClause.length()-1);

        String selectClause = lineSepr + "select " + aggClause + lineSepr + fromClause
                + joinClause + lineSepr + orderByClause;

        System.out.println(withSqlClause + selectClause);
        return withSqlClause + selectClause;
    }

    public OlapResultSet queryData(OlapColumn[] olapColumns, int pageNum, int pageSize) {
        String sql = this.produceSql(olapColumns, "", null);
        return postgresqlJdbcTemplate.query(
                (Connection conn) ->
                        conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY),
                (ResultSet rs) -> {
                    OlapResultSet olapResultSet = new OlapResultSet();
                    List<Map> rowSet = new ArrayList<>();
                    int rowUpperLimit = pageSize < 1 ? 0 : pageNum * pageSize;
                    int rowLowLimit = (pageSize < 1 || pageNum < 1) ? 1 : ((pageNum - 1) * pageSize + 1);
                    int rowQty = 0;
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colQty = rsmd.getColumnCount();
                    if (rs.last()) {
                        rowQty = rs.getRow();
                        if (rowUpperLimit > rowQty || rowUpperLimit == 0)
                            rowUpperLimit = rowQty;
                        rs.relative(rowLowLimit - rowQty - 1);
                        rs.setFetchSize(rowUpperLimit - rowLowLimit + 1);

                        for (int i = 1; (i <= rowUpperLimit - rowLowLimit + 1) && rs.next(); i++) {
                            Map linkedHashMap = new LinkedHashMap();
                            for (int j = 1; j <= colQty; j++)
                                linkedHashMap.put(rsmd.getColumnLabel(j), rs.getString(j));
                            linkedHashMap.put("序号", i);
                            rowSet.add(linkedHashMap);
                        }

                        olapResultSet.setRowSet(rowSet);
                        olapResultSet.setTotalRowQty(rowQty);
                        olapResultSet.setPageNum(pageNum);
                        olapResultSet.setPageSize(pageSize);
                        olapResultSet.setRowQty(rowUpperLimit - rowLowLimit + 1);
                    }
                    return olapResultSet;
                });
    }

    public OlapResultSet queryData(OlapColumn[] olapColumns){
        return this.queryData(olapColumns, 0, 0);
    }

}