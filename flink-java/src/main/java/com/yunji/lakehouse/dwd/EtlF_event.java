package com.yunji.lakehouse.dwd;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hudi.util.HoodiePipeline;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

class ProcFuncO_event extends ProcessFunction<String, Tuple4<Integer,Long,String,String>> implements CheckpointedFunction {
    private ListState<Long> ls = null;
    private ListStateDescriptor lsd = new ListStateDescriptor("r_ts",Long.class);
    private Long r_ts = System.currentTimeMillis();
    private OutputTag<Tuple4<Integer,Long,String,String>> o_event_histOutputTag = new OutputTag<Tuple4<Integer,Long,String,String>>("o_event_hist") {};
    private OutputTag<Tuple4<Integer,Long,String,String>> o_eventOutputTag = new OutputTag<Tuple4<Integer,Long,String,String>>("o_event") {};

    @Override
    public void processElement(String s, ProcessFunction<String, Tuple4<Integer,Long,String,String>>.Context context, Collector<Tuple4<Integer,Long,String,String>> collector) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        String op = jsonObject.getString("op");
        op = op.equals("c")?"i":op;
        JSONObject data = null;
        if(op.equals("d"))
            data = jsonObject.getJSONObject("before");
        else
            data = jsonObject.getJSONObject("after");
        JSONObject source = jsonObject.getJSONObject("source");
        long ts_ms = source.getLongValue("ts_ms");
        data.replace("event_time",data.getLongValue("event_time")-28800000);
        Tuple4<Integer,Long,String,String> rec = new Tuple4(data.getIntValue("id"),ts_ms,op,data.toJSONString());
        context.output(o_event_histOutputTag,rec);
        context.output(o_eventOutputTag,rec);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        ls.clear();
        ls.add(r_ts);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        StateTtlConfig ttlConf = StateTtlConfig.newBuilder(Time.seconds(10L)).updateTtlOnCreateAndWrite().neverReturnExpired().build();
        lsd.enableTimeToLive(ttlConf);
        ls = functionInitializationContext.getOperatorStateStore().getListState(lsd);
        if(functionInitializationContext.isRestored())
            r_ts = ls.get().iterator().next();
    }
}

public class EtlF_event {
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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,2000L));
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///D:/checkpoints/"));
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(200);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.getConfig().getConfiguration().setString("pipeline.name","product F_event");

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode","initial");
        debeziumProperties.put("time.precision.mode","connect");
        //debeziumProperties.put("snapshot.mode","never");
        debeziumProperties.put("snapshot.fetch.size","1000");
        debeziumProperties.put("scan.incremental.snapshot.enabled","false");
        //debeziumProperties.put("scan.incremental.snapshot.chunk.size","1000");

        JdbcConnectionOptions srcConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=src")
                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();
        JdbcConnectionOptions odsConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=ods")
                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();
        JdbcConnectionOptions dwdConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=dwd")
                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();
        JdbcConnectionOptions dwsConnOpt = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=dws")
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
                "event_time AS localtimestamp, \n" +
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

        DataStream<Row> datastream = tabEnv.toDataStream(resultData);
        datastream.addSink(JdbcSink.sink("insert into event(id,event_type,event_time,uid,page) values(?,?,?,?,?) " +
                        "on conflict(id) " +
                        "do nothing",
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Row row) throws SQLException {
                        pstmt.setInt(1, row.<Integer>getFieldAs("id"));
                        pstmt.setString(2,row.<String>getFieldAs("event_type"));
                        pstmt.setTimestamp(3,Timestamp.valueOf(row.<LocalDateTime>getFieldAs("event_time")));
                        pstmt.setInt(4, row.<Integer>getFieldAs("uid"));
                        pstmt.setString(5, row.<String>getFieldAs("page"));
                    }
                },
                rtExecOpt,srcConnOpt
        ));

         */

        SingleOutputStreamOperator<Tuple4<Integer,Long,String,String>> process = env.addSource(cdcBldr.schemaList("src").tableList("src.event").slotName("slot_event_12031952").build())
                .process(new ProcFuncO_event());

        OutputTag<Tuple4<Integer,Long,String,String>> o_event_histOutputTag = new OutputTag<Tuple4<Integer,Long,String,String>>("o_event_hist") {};
        OutputTag<Tuple4<Integer,Long,String,String>> o_eventOutputTag = new OutputTag<Tuple4<Integer,Long,String,String>>("o_event") {};

        process.getSideOutput(o_eventOutputTag).addSink(JdbcSink.sink(
                "insert into o_event(id,ts_dt,ts_ms,op,data) values(?,?,?,?,?::jsonb) " +
                        "on conflict(id) " +
                        "do update set ts_dt=excluded.ts_dt,ts_ms=EXCLUDED.ts_ms,op=EXCLUDED.op,data=EXCLUDED.data::jsonb",
                new JdbcStatementBuilder<Tuple4<Integer,Long,String,String>>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Tuple4<Integer,Long,String,String> rec) throws SQLException {
                        pstmt.setInt(1, rec.f0);
                        pstmt.setDate(2, new Date(rec.f1));
                        pstmt.setTimestamp(3,new Timestamp(rec.f1));
                        pstmt.setString(4,rec.f2);
                        pstmt.setString(5, rec.f3);
                    }
                },
                slowExecOpt,odsConnOpt
        ));

        HoodiePipeline.Builder BldrO_event = HoodiePipeline.builder("o_event_hist")
                .column("id int").column("ts_dt varchar(10)").column("ts_ms timestamp").column("op varchar(10)").column("data varchar(1000)")
                .pk("id","ts_ms","op").partition("ts_dt")
                .option("path","file:///D:/hudi/o_event_hist")
                .option("table.type","MERGE_ON_READ")
                .option("compaction.tasks","1")
                .option("precombine.field","ts_ms")
                .option("write.tasks","1")
                .option("compaction.trigger.strategy","time_elapsed")
                .option("compaction.delta_seconds","10")
                .option("partition.path.field","ts_dt")
                //.option("write.datetime.partitioning","true")
                //.option("write.partition.format","yyyyMMdd")
                //.option("hive_sync.enable","true")
                //.option("hive_sync.mode","hms")
                //.option("hive_sync.metastore.uris","thrift://localhost:9083")
                //.option("hive_sync.table","o_event_hist")
                //.option("hive_sync.username","postgres")
                //.option("hive_sync.password","postgres")
                //.option("hive_sync.support_timestamp","true")
                ;

        SingleOutputStreamOperator<RowData> map = process.getSideOutput(o_event_histOutputTag).map(t -> {
            GenericRowData rowData = new GenericRowData(5);
            rowData.setField(0, t.f0);
            rowData.setField(1, StringData.fromString(TimestampData.fromEpochMillis(t.f1).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))));
            rowData.setField(2, TimestampData.fromEpochMillis(t.f1));
            rowData.setField(3, StringData.fromString(t.f2));
            rowData.setField(4, StringData.fromString(t.f3));
            return rowData;
        });
        BldrO_event.sink(map,false);

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
                "'slot.name'='slot_event_666',\n" +
                "'debezium.snapshot.select.statement.overrides'='src.event',\n" +
                "'debezium.snapshot.select.statement.overrides.src.event'='select * from src.event order by id'\n" +
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

        String sqlTemp ="select e.id,cast(e.event_time as date) event_date,e.event_type,e.event_time,e.uid,u.name user_name,u.age user_age,e.page," +
                "cast(null as timestamp) as session_start_time,cast(null as timestamp) as session_end_time,cast(null as bigint) as session_second," +
                "cast(null as int) as session_page_offset,cast(null as int) as session_event_offset,cast(null as int) as session_page,cast(null as varchar) as session_path," +
                "cast(null as varchar) as session_start_page,cast(null as varchar) as session_start_event,cast(null as int) as session_event," +
                "cast(null as varchar) as session_end_page,cast(null as varchar) as session_end_event " +
                "from cdcEvent e left join jdbcD_user for system_time as of e.proc_time as u on e.uid=u.uid";

        Table f_event = tabEnv.sqlQuery(sqlTemp);

        DataStream<Row> streamF_event = tabEnv.toChangelogStream(f_event);

        SingleOutputStreamOperator<List<Row>> process1 = streamF_event.assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Row>() {
                            @Override
                            public long extractTimestamp(Row row, long l) {
                                return row.<LocalDateTime>getFieldAs("event_time").toInstant(ZoneOffset.of("+8")).toEpochMilli();
                            }
                        }))
                .keyBy(new KeySelector<Row, Integer>() {
                    @Override
                    public Integer getKey(Row row) throws Exception {
                        return row.<Integer>getFieldAs("uid");
                    }
                })
                .window(EventTimeSessionWindows.withGap(org.apache.flink.streaming.api.windowing.time.Time.seconds(30L)))
                .process(new ProcessWindowFunction<Row, List<Row>, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Row, List<Row>, Integer, TimeWindow>.Context context, Iterable<Row> iterable, Collector<List<Row>> collector) throws Exception {
                        LocalDateTime session_start_time = LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()), ZoneId.systemDefault());
                        LocalDateTime session_end_time = LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()), ZoneId.systemDefault());
                        long session_second = Duration.between(session_start_time, session_end_time).toMillis()/1000;
                        String session_path = "";
                        String last_page = "";
                        int session_page_offset = 0;
                        int session_event_offset = 0;
                        List<Row> rows = new ArrayList<Row>();
                        for (Row r : iterable)
                            rows.add(r);
                        rows.sort((o1, o2) -> {
                            return o1.<LocalDateTime>getFieldAs("event_time").compareTo(o2.<LocalDateTime>getFieldAs("event_time"));
                        });
                        for (Row row : rows) {
                            if (row.getKind().name() == "INSERT" || row.getKind().name() == "UPDATE_AFTER") {
                                String curr_page = row.<String>getFieldAs("page");
                                if (last_page == null || !curr_page.equals(last_page)) {
                                    session_path += ("->" + curr_page);
                                    session_page_offset ++;
                                    last_page = curr_page;
                                }
                                session_event_offset ++;
                                row.setField("session_page_offset", session_page_offset);
                                row.setField("session_event_offset", session_event_offset);
                                row.setField("session_path", session_path.substring(2));
                                //System.out.println("row:" + row.toString());
                            }
                        }
                        Row last = rows.get(rows.size()-1);
                        Row first = rows.get(0);
                        int session_page = last.<Integer>getFieldAs("session_page_offset");
                        int session_event = last.<Integer>getFieldAs("session_event_offset");
                        String session_end_page = last.<String>getFieldAs("page");
                        String session_end_event = last.<String>getFieldAs("event_type");
                        String session_start_page = first.<String>getFieldAs("page");
                        String session_start_event = first.<String>getFieldAs("event_type");
                        for (Row row : rows) {
                            row.setField("session_page", session_page);
                            row.setField("session_event", session_event);
                            row.setField("session_start_time", session_start_time);
                            row.setField("session_end_time", session_end_time);
                            row.setField("session_second", session_second);
                            row.setField("session_start_page", session_start_page);
                            row.setField("session_start_event", session_start_event);
                            row.setField("session_end_page",session_end_page);
                            row.setField("session_end_event",session_end_event);
                            if (row.<LocalDateTime>getFieldAs("session_start_time") == null)
                                System.out.println("null session_start_time");
                        }
                        collector.collect(rows);
                    }
                });

        process1.<Row>flatMap((FlatMapFunction<List<Row>, Row>) (rows, collector) -> {
                    for (Row row : rows) collector.collect(row);})
                .returns(Row.class)
                .addSink(JdbcSink.sink(
                        "insert into f_event(id,event_date,event_type,event_time,uid,user_name,user_age,page," +
                                "session_start_time,session_end_time,session_second,session_page_offset,session_event_offset," +
                                "session_page,session_path,session_event,sys_update_time) " +
                                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp) on conflict(id) " +
                                "do update set event_date=excluded.event_date,event_type=excluded.event_type," +
                                "event_time=excluded.event_time,uid=excluded.uid,user_name=excluded.user_name," +
                                "user_age=excluded.user_age,page=excluded.page,session_start_time=excluded.session_start_time," +
                                "session_end_time=excluded.session_end_time,session_second=excluded.session_second," +
                                "session_page_offset=excluded.session_page_offset,session_event_offset=excluded.session_event_offset," +
                                "session_page=excluded.session_page,session_path=excluded.session_path,session_event=excluded.session_event," +
                                "sys_update_time=excluded.sys_update_time ",
                        new JdbcStatementBuilder<Row>() {
                            @Override
                            public void accept(PreparedStatement pstmt, Row row) throws SQLException {
                                pstmt.setInt(1, row.<Integer>getFieldAs("id"));
                                pstmt.setDate(2, Date.valueOf(row.<LocalDate>getFieldAs("event_date")));
                                pstmt.setString(3, row.<String>getFieldAs("event_type"));
                                pstmt.setTimestamp(4, Timestamp.valueOf(row.<LocalDateTime>getFieldAs("event_time")));
                                pstmt.setInt(5, row.<Integer>getFieldAs("uid"));
                                pstmt.setString(6, row.<String>getFieldAs("user_name"));
                                pstmt.setInt(7, row.<Integer>getFieldAs("user_age"));
                                pstmt.setString(8, row.<String>getFieldAs("page"));
                                pstmt.setTimestamp(9, Timestamp.valueOf(row.<LocalDateTime>getFieldAs("session_start_time")));
                                pstmt.setTimestamp(10, Timestamp.valueOf(row.<LocalDateTime>getFieldAs("session_end_time")));
                                pstmt.setLong(11, row.<Long>getFieldAs("session_second"));
                                pstmt.setInt(12, row.<Integer>getFieldAs("session_page_offset"));
                                pstmt.setInt(13, row.<Integer>getFieldAs("session_event_offset"));
                                pstmt.setInt(14, row.<Integer>getFieldAs("session_page"));
                                pstmt.setString(15, row.<String>getFieldAs("session_path"));
                                pstmt.setInt(16, row.<Integer>getFieldAs("session_event"));
                            }
                        },
                        fastExecOpt, dwdConnOpt
                ));

        process1.map(new MapFunction<List<Row>, Row>() {
            @Override
            public Row map(List<Row> rows) throws Exception {
                if(rows.get(rows.size() - 1).<Integer>getFieldAs("uid") == 5 )
                    System.out.println(rows.get(rows.size() - 1).toString());
                return rows.get(rows.size() - 1);
            }})
                .returns(Row.class)
                .addSink(JdbcSink.sink(
                "insert into s_event_session(uid,session_start_time,session_end_time,user_name,user_age," +
                        "session_second,session_page,session_start_page,session_end_page,session_path,session_event," +
                        "session_start_event,session_end_event,sys_update_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp) " +
                        "on conflict(uid,session_start_time,session_end_time) do update set user_name=excluded.user_name,user_age=excluded.user_age," +
                        "session_second=excluded.session_second,session_page=excluded.session_page,session_start_page=excluded.session_start_page," +
                        "session_end_page=excluded.session_end_page,session_path=excluded.session_path,session_event=excluded.session_event," +
                        "session_start_event=excluded.session_start_event,session_end_event=excluded.session_end_event," +
                        "sys_update_time=excluded.sys_update_time",
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Row row) throws SQLException {
                        pstmt.setInt(1, row.<Integer>getFieldAs("uid"));
                        pstmt.setTimestamp(2, Timestamp.valueOf(row.<LocalDateTime>getFieldAs("session_start_time")));
                        pstmt.setTimestamp(3, Timestamp.valueOf(row.<LocalDateTime>getFieldAs("session_end_time")));
                        pstmt.setString(4, row.<String>getFieldAs("user_name"));
                        pstmt.setInt(5, row.<Integer>getFieldAs("user_age"));
                        pstmt.setLong(6, row.<Long>getFieldAs("session_second"));
                        pstmt.setInt(7, row.<Integer>getFieldAs("session_page"));
                        pstmt.setString(8, row.<String>getFieldAs("session_start_page"));
                        pstmt.setString(9, row.<String>getFieldAs("session_end_page"));
                        pstmt.setString(10, row.<String>getFieldAs("session_path"));
                        pstmt.setInt(11, row.<Integer>getFieldAs("session_event"));
                        pstmt.setString(12, row.<String>getFieldAs("session_start_event"));
                        pstmt.setString(13, row.<String>getFieldAs("session_end_event"));
                    }
                },
                rtExecOpt, dwsConnOpt
        ));

        DataStreamSource<String> stringDataStreamSource = env.addSource(cdcBldr.schemaList("dim").tableList("dim.d_user").slotName("slot_d_user_9").build());

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