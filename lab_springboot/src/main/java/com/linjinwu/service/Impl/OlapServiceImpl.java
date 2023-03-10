package com.linjinwu.service.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.linjinwu.dao.OlapDao;
import com.linjinwu.pojo.OlapColumn;
import com.linjinwu.pojo.OlapResultSet;
import com.linjinwu.service.OlapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
public class OlapServiceImpl implements OlapService {
    @Autowired
    private OlapDao olapDao;
    public String queryData(){
        List<OlapColumn> olapColumns = new ArrayList<OlapColumn>();
        OlapColumn olapColumn = new OlapColumn();
        olapColumn.setQueryName("用户名称");
        olapColumn.setTableName("f_order");
        olapColumn.setAggColName("user_name");
        olapColumn.setAggOp("max");
        olapColumn.setAggColDataType(2);
        olapColumn.setGroupByColNames(new String[]{"user_id","prod_id"});
        olapColumns.add(olapColumn);
        OlapColumn olapColumn2 = new OlapColumn();
        olapColumn2.setQueryName("产品名称");
        olapColumn2.setTableName("f_order");
        olapColumn2.setAggColName("prod_type");
        olapColumn2.setAggColDataType(2);
        olapColumn2.setAggOp("min");
        olapColumn2.setGroupByColNames(new String[]{"user_id","prod_id"});
        olapColumns.add(olapColumn2);
        OlapColumn olapColumn3 = new OlapColumn();
        olapColumn3.setQueryName("订单量");
        olapColumn3.setAggColName("ord_qty");
        olapColumn3.setSortTag(-1);
        olapColumn3.setTableName("f_order");
        olapColumn3.setAggOp("sum");
        olapColumn3.setAggColDataType(1);
        olapColumn3.setGroupByColNames(new String[]{"user_id","prod_id"});
        olapColumns.add(olapColumn3);
        OlapColumn olapColumn4 = new OlapColumn();
        olapColumn4.setQueryName("ljw订单额");
        olapColumn4.setAggColName("ord_amt");
        olapColumn4.setWhereClause("user_name='linjinwu'");
        olapColumn4.setSortTag(3);
        olapColumn4.setTableName("f_order");
        olapColumn4.setAggOp("max");
        olapColumn4.setAggColDataType(1);
        olapColumn4.setGroupByColNames(new String[]{"user_id","prod_id"});
        olapColumns.add(olapColumn4);
        OlapColumn olapColumn5 = new OlapColumn();
        olapColumn5.setQueryName("订单人数");
        olapColumn5.setAggColName("user_qty");
        olapColumn5.setSortTag(-2);
        olapColumn5.setTableName("f_order");
        olapColumn5.setAggOp("countDist");
        olapColumn5.setAggColDataType(0);
        olapColumn5.setGroupByColNames(new String[]{"user_id","prod_id"});
        olapColumns.add(olapColumn5);
        OlapResultSet olapResultSet = olapDao.queryData(olapColumns.toArray(new OlapColumn[0]));
        System.out.println("pageNum is " + olapResultSet.getPageNum());
        System.out.println("pageSize is " + olapResultSet.getPageSize());
        System.out.println("totalRowQty is " + olapResultSet.getTotalRowQty());
        System.out.println("rowQty is " + olapResultSet.getRowQty());
        System.out.println(JSONArray.toJSONString(olapResultSet.getRowSet()));
        return "succeed!";
    }
}
