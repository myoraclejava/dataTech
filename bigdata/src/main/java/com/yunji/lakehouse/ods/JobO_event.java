package com.yunji.lakehouse.ods;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yunji.lakehouse.pojo.Event;
import com.yunji.lakehouse.pojo.O_event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Properties;

class MyProcessFunction extends ProcessFunction<String, O_event> implements CheckpointedFunction {
    private ListState<Long> ls = null;
    private ListStateDescriptor lsd = new ListStateDescriptor("r_ts",Long.class);
    private Long r_ts = System.currentTimeMillis();
    private OutputTag<O_event> o_event_histOutputTag = new OutputTag<O_event>("o_event_hist") {};
    private OutputTag<O_event> o_eventOutputTag = new OutputTag<O_event>("o_event") {};
    private OutputTag<O_event> o_event_changelogOutputTag = new OutputTag<O_event>("o_event_changelog") {};
    @Override
    public void processElement(String s, ProcessFunction<String, O_event>.Context context, Collector<O_event> collector) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        String op = jsonObject.getString("op");
        op = op.equals("c")?"i":op;
        JSONObject before = jsonObject.getJSONObject("before");
        JSONObject after = jsonObject.getJSONObject("after");
        JSONObject source = jsonObject.getJSONObject("source");
        long ts_ms = source.getLongValue("ts_ms");
        after.replace("event_time",after.getLongValue("event_time")-28800000);
        if(before!=null && !before.isEmpty()) {
            O_event beforeO_event_changelog = new O_event(before.getIntValue("id"),op.equals("r")?r_ts:ts_ms,op.equals("u")?"u_d":op,before.getString("event_type"),
                    before.getLongValue("event_time"),before.getIntValue("uid"), before.getString("page"),-1, before.toJSONString());
            context.output(o_event_changelogOutputTag,beforeO_event_changelog);
        }
        if(after!=null && !after.isEmpty()) {
            O_event afterO_event_changelog = new O_event(after.getIntValue("id"),op.equals("r")?r_ts:ts_ms,op.equals("u")?"u_i":op,after.getString("event_type"),
                    after.getLongValue("event_time"),after.getIntValue("uid"), after.getString("page"),1, after.toJSONString());
            O_event afterO_event = new O_event(after.getIntValue("id"),ts_ms,op,after.toJSONString());
            context.output(o_event_changelogOutputTag,afterO_event_changelog);
            context.output(o_event_histOutputTag,afterO_event);
            context.output(o_eventOutputTag,afterO_event);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        ls.clear();
        ls.add(r_ts);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ls = functionInitializationContext.getOperatorStateStore().getListState(lsd);
        if(functionInitializationContext.isRestored())
            r_ts = ls.get().iterator().next();
    }
}

public class JobO_event {
    //private static final Logger log = LoggerFactory.getLogger(JobO_event.class);
    public static void main(String[] args) throws Exception {
        //log.isInfoEnabled();
        //log.isTraceEnabled();
        // set up the streaming execution environment
        Configuration conf = new Configuration();
        conf.setInteger("taskmanager.numberOfTaskSlots",4);
        conf.setBoolean("pipeline.operator-chaining",false);
        conf.setString("rest.bind-port","8082");
        conf.setString("taskmanager.memory.task.heap.size","512mb");
        conf.setString("taskmanager.memory.task.off-heap.size","128mb");

        conf.setInteger("task.cancellation.timeout",0);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,2000L));
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        //env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///D:/checkpoints/"));
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(200);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.getConfig().getConfiguration().setString("pipeline.name","generate event data");

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode","initial");
        debeziumProperties.put("time.precision.mode","connect");
        //debeziumProperties.put("snapshot.mode","never");
        debeziumProperties.put("snapshot.fetch.size","1000");
        debeziumProperties.put("scan.incremental.snapshot.enabled","true");
        debeziumProperties.put("scan.incremental.snapshot.chunk.size","1000");

        JdbcConnectionOptions srcConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=src")
                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();
        JdbcConnectionOptions odsConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=ods")
                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();
        JdbcConnectionOptions dwdConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=dwd")
                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();
        JdbcExecutionOptions slowExecOpt = JdbcExecutionOptions.builder().withBatchIntervalMs(2000).withBatchSize(2000).withMaxRetries(20).build();
        JdbcExecutionOptions fastExecOpt = JdbcExecutionOptions.builder().withBatchIntervalMs(1000).withBatchSize(100).withMaxRetries(20).build();
        JdbcExecutionOptions rtExecOpt = JdbcExecutionOptions.builder().withBatchIntervalMs(0).withBatchSize(1).withMaxRetries(20).build();
        PostgreSQLSource.Builder<String> cdcBldr = PostgreSQLSource.<String>builder().username("postgres").password("postgres")
                .hostname("localhost").port(5432).database("postgres").decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema()).debeziumProperties(debeziumProperties);

        /*
        String cdcEvent_raw = "create table cdcEvent_raw(\n" +
                "id int, \n" + "" +
                "event_type_id int, \n" +
                "event_time AS UNIX_TIMESTAMP()*1000, \n" +
                "uid int, \n" +
                "page string, \n" +
                "proc_time as proctime()" +
                ") with (" +
                "'connector' = 'datagen',\n" +
                "'rows-per-second' = '1000',\n" +
                "'number-of-rows' = '1000000',\n" +
                "'fields.id.kind' = 'sequence',\n" +
                "'fields.id.start' = '1',\n" +
                "'fields.id.end' = '1000001',\n" +
                "'fields.uid.kind' = 'random',\n" +
                "'fields.uid.min' = '1',\n" +
                "'fields.uid.max' = '10',\n" +
                "'fields.event_type_id.kind' = 'random',\n" +
                "'fields.event_type_id.min' = '1',\n" +
                "'fields.event_type_id.max' = '3',\n" +
                "'fields.page.kind' = 'random',\n" +
                "'fields.page.length' = '1'\n" +
                ")";

        tabEnv.executeSql(cdcEvent_raw);

        String jdbcEvent_type = "create table jdbcEvent_type(" +
                "id int, \n" +
                "type string, \n" +
                "primary key(id) not enforced \n" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:postgresql://localhost:5432/postgres?currentSchema=src'," +
                "'username' = 'postgres'," +
                "'password' = 'postgres'," +
                "'table-name' = 'event_type'," +
                "'lookup.cache.ttl' = '600s'," +
                "'lookup.cache.max-rows' = '10'" +
                ")";

        tabEnv.executeSql(jdbcEvent_type);

        String DQLresult= "select e.id, et.type as event_type, e.event_time, e.uid, e.page \n" +
                "from cdcEvent_raw e \n" +
                "left join jdbcEvent_type for SYSTEM_TIME AS OF e.proc_time et \n" +
                "on e.event_type_id=et.id ";

        Table resultData = tabEnv.sqlQuery(DQLresult);
        //tabEnv.createTemporaryView("resultData",resultData);

        DataStream<Event> datastream = tabEnv.toDataStream(resultData, Event.class);
        datastream.addSink(JdbcSink.sink("insert into event(id,event_type,event_time,uid,page) values(?,?,?,?,?) " +
                        "on conflict(id) " +
                        "do nothing",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Event event) throws SQLException {
                        pstmt.setInt(1, event.getId());
                        pstmt.setString(2,event.getEvent_type());
                        pstmt.setTimestamp(3,new Timestamp(event.getEvent_time()));
                        pstmt.setInt(4, event.getUid());
                        pstmt.setString(5, event.getPage());
                    }
                },
                rtExecOpt,srcConnOpt
        ));

         */

        SingleOutputStreamOperator<O_event> process = env.addSource(cdcBldr.schemaList("src").tableList("src.event").slotName("slot_event_1958").build())
                .process(new MyProcessFunction());
        process.print();
        OutputTag<O_event> o_event_histOutputTag = new OutputTag<O_event>("o_event_hist") {};
        OutputTag<O_event> o_eventOutputTag = new OutputTag<O_event>("o_event") {};
        //OutputTag<O_event> o_event_changelogOutputTag = new OutputTag<O_event>("o_event_changelog") {};

        /*
        process.getSideOutput(o_event_changelogOutputTag).addSink(JdbcSink.sink(
                "insert into o_event_changelog(id,ts_ms,op,event_type,event_time,uid,page,event_qty,data) values(?,?,?,?,?,?,?,?,?::jsonb) ",
                new JdbcStatementBuilder<O_event>() {
                    @Override
                    public void accept(PreparedStatement pstmt, O_event o_event) throws SQLException {
                        pstmt.setInt(1, o_event.getId());
                        pstmt.setTimestamp(2,new Timestamp(o_event.getTs_ms()));
                        pstmt.setString(3,o_event.getOp());
                        pstmt.setString(4, o_event.getEvent_type());
                        pstmt.setTimestamp(5, new Timestamp(o_event.getEvent_time()));
                        pstmt.setInt(6, o_event.getUid());
                        pstmt.setString(7, o_event.getPage());
                        pstmt.setInt(8,o_event.getEvent_qty());
                        pstmt.setString(9,o_event.getData());
                    }
                },
                slowExecOpt,odsConnOpt
        ));
         */

        /*
        process.getSideOutput(o_event_histOutputTag).addSink(JdbcSink.sink(
                "insert into o_event_hist(id,ts_ms,op,data) values(?,?,?,?::jsonb) " +
                        "on conflict(id,ts_ms,op) " +
                        "do update set data=EXCLUDED.data::jsonb",
                new JdbcStatementBuilder<O_event>() {
                    @Override
                    public void accept(PreparedStatement pstmt, O_event o_event) throws SQLException {
                        pstmt.setInt(1, o_event.getId());
                        pstmt.setTimestamp(2,new Timestamp(o_event.getTs_ms()));
                        pstmt.setString(3,o_event.getOp());
                        pstmt.setString(4, o_event.getData());
                    }
                },
                slowExecOpt,odsConnOpt
        ));
        */

        process.getSideOutput(o_eventOutputTag).addSink(JdbcSink.sink(
                "insert into o_event(id,ts_dt,ts_ms,op,data) values(?,?,?,?,?::jsonb) " +
                        "on conflict(id) " +
                        "do update set ts_dt=excluded.ts_dt,ts_ms=EXCLUDED.ts_ms,op=EXCLUDED.op,data=EXCLUDED.data::jsonb",
                new JdbcStatementBuilder<O_event>() {
                    @Override
                    public void accept(PreparedStatement pstmt, O_event o_event) throws SQLException {
                        pstmt.setInt(1, o_event.getId());
                        pstmt.setDate(2, new Date(o_event.getTs_ms()));
                        pstmt.setTimestamp(3,new Timestamp(o_event.getTs_ms()));
                        pstmt.setString(4,o_event.getOp());
                        pstmt.setString(5, o_event.getData());
                    }
                },
                slowExecOpt,odsConnOpt
        ));

        HashMap<String, String> opt = new HashMap<String, String>();
        opt.put(FlinkOptions.PATH.key(),"file:///D:/hudi");
        opt.put(FlinkOptions.TABLE_NAME.key(),"o_event_hist");
        opt.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        opt.put(FlinkOptions.COMPACTION_TASKS.key(), "1");
        opt.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts_ms");
        //opt.put(FlinkOptions.RECORD_KEY_FIELD.key(),"id");
        opt.put(FlinkOptions.WRITE_TASKS.key(),"1");
        opt.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(),"3");
        //opt.put(FlinkOptions.COMPACTION_DELTA_SECONDS.key(),"5");
        opt.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
        opt.put(FlinkOptions.READ_START_COMMIT.key(), "20221111083026792");
        //opt.put(FlinkOptions.HIVE_SYNC_ENABLED.key(), "true");
        //opt.put(FlinkOptions.HIVE_SYNC_MODE.key(), "hms");
        //opt.put(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(), "thrift://localhost:9083");
        //opt.put(FlinkOptions.HIVE_SYNC_JDBC_URL,"")
        //opt.put(FlinkOptions.HIVE_SYNC_TABLE.key(), "o_event");
        //opt.put(FlinkOptions.HIVE_SYNC_USERNAME.key(),"postgres");
        //opt.put(FlinkOptions.HIVE_SYNC_PASSWORD.key(),"postgres");
        //opt.put(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP.key(),"true");

        HoodiePipeline.Builder BldrO_event = HoodiePipeline.builder("o_event_hist")
                .column("id int").column("ts_dt varchar(10)").column("ts_ms timestamp").column("op varchar(10)").column("data varchar(1000)")
                .pk("id","ts_ms","op")
                .partition("ts_dt").options(opt);

        SingleOutputStreamOperator<RowData> map = process.getSideOutput(o_event_histOutputTag).map(t -> {
            GenericRowData rowData = new GenericRowData(5);
            rowData.setField(0, t.getId());
            //System.out.println(TimestampData.fromEpochMillis(t.getTs_ms()).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            //rowData.setField(1, TimestampData.fromEpochMillis(t.getTs_ms()).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            rowData.setField(1,StringData.fromString(TimestampData.fromEpochMillis(t.getTs_ms()).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))));
            rowData.setField(2, TimestampData.fromEpochMillis(t.getTs_ms()));
            rowData.setField(3, StringData.fromString(t.getOp()));
            rowData.setField(4, StringData.fromString(t.getData()));
            return rowData;
        });
        BldrO_event.sink(map,false);

        //BldrO_event.source(env).print();

        String cdcEvent = "create table cdcEvent(\n" +
                "id int,\n" +
                "event_type varchar,\n" +
                "event_time timestamp,\n" +
                "uid int,\n" +
                "page varchar,\n" +
                "proc_time as proctime(),\n" +
                "primary key(id) not enforced\n" +
                ") with (\n" +
                "'connector' = 'postgres-cdc',\n" +
                "'hostname' = 'localhost',\n" +
                "'port'='5432',\n" +
                "'database-name'='postgres',\n" +
                "'schema-name'='src',\n" +
                "'table-name'='event',\n" +
                "'username'='postgres',\n" +
                "'password'='postgres',\n" +
                "'decoding.plugin.name'='pgoutput',\n" +
                "'slot.name'='slot_event_4'\n" +
        ")";

        tabEnv.executeSql(cdcEvent);

        String jdbcD_user = "create table jdbcD_user(\n" +
                "uid int," +
                "name varchar," +
                "age int," +
                "primary key(uid) not enforced\n" +
                ") with (" +
                "'connector' = 'jdbc',\n" +
                "'url'='jdbc:postgresql://localhost:5432/postgres?currentSchema=dim',\n" +
                "'table-name'='d_user',\n" +
                "'username'='postgres',\n" +
                "'password'='postgres'\n" +
        ")";

        tabEnv.executeSql(jdbcD_user);

        String sqlTemp ="select e.id,cast(e.event_time as date) event_date,e.event_type,e.event_time,e.uid,u.name user_name,u.age user_age,e.page " +
                "from cdcEvent e left join jdbcD_user for system_time as of e.proc_time as u on e.uid=u.uid";

        Table f_event = tabEnv.sqlQuery(sqlTemp);

        DataStream<Row> streamF_event = tabEnv.toChangelogStream(f_event);
                //.toDataStream(f_event);

        streamF_event.addSink(JdbcSink.sink(
                "insert into f_event(id,event_date,event_type,event_time,uid,user_name,user_age,page) " +
                        "values(?,?,?,?,?,?,?,?) on conflict(id) " +
                        "do update set event_date=excluded.event_date,event_type=excluded.event_type," +
                        "              event_time=excluded.event_time,uid=excluded.uid,user_name=excluded.user_name," +
                        "              user_age=excluded.user_age,page=excluded.page",
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Row row) throws SQLException {
                        pstmt.setInt(1,row.<Integer>getFieldAs(0));
                        pstmt.setDate(2,Date.valueOf(row.<LocalDate>getFieldAs(1)));
                        pstmt.setString(3,row.<String>getFieldAs(2));
                        pstmt.setTimestamp(4,Timestamp.valueOf(row.<LocalDateTime>getFieldAs(3)));
                        pstmt.setInt(5,row.<Integer>getFieldAs(4));
                        pstmt.setString(6,row.<String>getFieldAs(5));
                        pstmt.setInt(7,row.<Integer>getFieldAs(6));
                        pstmt.setString(8,row.<String>getFieldAs(7));
                    }
                },
                fastExecOpt,dwdConnOpt
        ));

        DataStreamSource<String> stringDataStreamSource = env.addSource(cdcBldr.schemaList("dim").tableList("dim.d_user").slotName("slot_d_user_9").build());
        stringDataStreamSource.print();
        stringDataStreamSource.addSink(JdbcSink.sink(
                        "update f_event set user_name=?,user_age=? where uid=? and (user_name<>? or user_age<>?)",
                        new JdbcStatementBuilder<String>() {
                            @Override
                            public void accept(PreparedStatement pstmt, String str) throws SQLException {
                                JSONObject jsonObject = JSONObject.parseObject(str).getJSONObject("after");
                                pstmt.setString(1,jsonObject.getString("name"));
                                //pstmt.setInt(3,jsonObject.getIntValue("age"));
                                pstmt.setInt(2,jsonObject.getIntValue("age"));
                                pstmt.setInt(3,jsonObject.getIntValue("uid"));
                                pstmt.setString(4,jsonObject.getString("name"));
                                pstmt.setInt(5,jsonObject.getIntValue("age"));
                            }
                        },
                        rtExecOpt,dwdConnOpt
                ));

        env.execute("ETL job for o_event,o_event_hist,o_event_changelog");

    }
}
