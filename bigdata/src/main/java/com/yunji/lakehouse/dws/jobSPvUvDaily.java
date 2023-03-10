package com.yunji.lakehouse.dws;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yunji.lakehouse.pojo.UserDailyPv;
import com.yunji.lakehouse.util.FileSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;

public class jobSPvUvDaily {
    //计算每天8点至次日8点每个用户的PV，每分钟输出统计结果
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws Exception {
        //获取env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //设置并行度
        env.setParallelism(1);
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        //设置checkpoint
        env.enableCheckpointing(3000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置恢复策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,3));
        //设置水位
        env.getConfig().setAutoWatermarkInterval(2000L);
        //读取json
        SingleOutputStreamOperator<UserDailyPv> returns =
                env.addSource(new FileSource("D:\\",10,5000))
                //env.readFile(new TextInputFormat(null), "file:///D:/", FileProcessingMode.PROCESS_CONTINUOUSLY, 5000L)
                .map(t -> JSONObject.parseObject(t,UserDailyPv.class))
                .returns(UserDailyPv.class);
        //添加水位
        SingleOutputStreamOperator<UserDailyPv> tuple3SingleOutputStreamOperator = returns
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserDailyPv>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserDailyPv>() {
                            private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            @Override
                            public long extractTimestamp(UserDailyPv stringLongIntegerTuple3, long l) {
                                try {
                                    return df.parse(stringLongIntegerTuple3.getEventTime()).getTime();
                                } catch (ParseException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }));
        //keyBY
        KeyedStream<UserDailyPv, String> tuple3ObjectKeyedStream =
                tuple3SingleOutputStreamOperator.keyBy(new KeySelector<UserDailyPv, String>() {
                    @Override
                    public String getKey(UserDailyPv stringLongIntegerTuple3) throws Exception {
                        //System.out.println("getKey:" + stringLongIntegerTuple3.getUid());
                        return stringLongIntegerTuple3.getUid();
                    }
                });
        //开窗
        WindowedStream<UserDailyPv, String, TimeWindow> window = tuple3ObjectKeyedStream.window(TumblingEventTimeWindows.of(Time.minutes(2)));
        //设置触发器
        WindowedStream trigger = window.trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)));
        //设置延时
        WindowedStream windowedStream = trigger.allowedLateness(Time.minutes(2));
        //reduce
        SingleOutputStreamOperator reduce = windowedStream.reduce(new ReduceFunction<UserDailyPv>() {
            @Override
            public UserDailyPv reduce(UserDailyPv o, UserDailyPv t1) throws Exception {
                System.out.println("reduce:" + o.toString() + "---" + t1.toString());
                return o.setPv(o.getPv()+t1.getPv());
            }
        }, new ProcessWindowFunction<UserDailyPv, UserDailyPv, String, TimeWindow>() {
            @Override
            public void process(String o, Context context, Iterable<UserDailyPv> iterable, Collector<UserDailyPv> collector) throws Exception {
                TimeWindow window1 = context.window();
                UserDailyPv next = iterable.iterator().next();
                UserDailyPv userDailyPv = next.setStartTime(window1.getStart()).setEndTime(window1.getEnd());
                collector.collect(userDailyPv);
                System.out.println(userDailyPv.toString());
            }
        });
        //添加sink
        reduce.addSink(JdbcSink.sink("insert into s_pv as d(stat_range,start_time,end_time,uid,pv,driver_time) values(?,?,?,?,?,?) " +
                        "on conflict(stat_range,start_time,end_time,uid) " +
                        "do update set pv=EXCLUDED.pv where d.driver_time<EXCLUDED.driver_time",
                new JdbcStatementBuilder<UserDailyPv>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, UserDailyPv o) throws SQLException {
                        preparedStatement.setString(1,"user_daily");
                        preparedStatement.setTimestamp(2, new Timestamp(o.getStartTime()));
                        preparedStatement.setTimestamp(3, new Timestamp(o.getEndTime()));
                        preparedStatement.setString(4, o.getUid());
                        preparedStatement.setInt(5, o.getPv());
                        preparedStatement.setTimestamp(6, new Timestamp(0));
                    }
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(1000).withBatchSize(1000).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=lab")
                        .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build()
        ));

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode","never");
        debeziumProperties.put("time.precision.mode","adaptive_time_microseconds");
        DebeziumSourceFunction<String> build = PostgreSQLSource.<String>builder().hostname("localhost").port(5432)
                .database("postgres").tableList("lab.s_pv").username("postgres").password("postgres").decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema()).debeziumProperties(debeziumProperties).build();

        DataStreamSource<String> hashMapDataStreamSource = env.addSource(build);

        OutputTag<UserDailyPv> userDailyOutputTag = new OutputTag<UserDailyPv>("userDailyPV") {};
        OutputTag<UserDailyPv> userHourlyOutputTag = new OutputTag<UserDailyPv>("userHourlyPV"){};
        OutputTag<UserDailyPv> dailyOutputTag = new OutputTag<UserDailyPv>("dailyPV") {};
        OutputTag<UserDailyPv> hourlyOutputTag = new OutputTag<UserDailyPv>("hourlyPV"){};

        SingleOutputStreamOperator<UserDailyPv> process = hashMapDataStreamSource.process(new ProcessFunction<String, UserDailyPv>() {
            @Override
            public void processElement(String string, ProcessFunction<String, UserDailyPv>.Context context, Collector<UserDailyPv> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(string);
                JSONObject source = jsonObject.getJSONObject("source");
                JSONObject before = jsonObject.getJSONObject("before");
                JSONObject after = jsonObject.getJSONObject("after");
                String op = jsonObject.getString("op");
                long refreshTs = source.getLong("ts_ms");
                String table = jsonObject.getString("table");
                String schema = jsonObject.getString("schema");
                HashMap<String, Object> stringObjectHashMap = new HashMap<String, Object>();
                if("d".equals(op))
                    after = before;
                op = "r".equals(op)?"c":op;
                stringObjectHashMap.put("statRange", after.getString("stat_range"));
                stringObjectHashMap.put("startTime", after.getLong("start_time"));
                stringObjectHashMap.put("endTime", after.getLong("end_time"));
                stringObjectHashMap.put("uid", after.getString("uid"));
                stringObjectHashMap.put("statTime", refreshTs);
                System.out.println("-----startTime: " + after.getLong("start_time"));
                System.out.println("-----statTime: " + refreshTs);
                stringObjectHashMap.put("op", op);
                stringObjectHashMap.put("driverTime", after.getLong("driver_time"));
                stringObjectHashMap.put("pv", after.getInteger("pv"));
                //System.out.println("-----stringObjectHashMap: " + stringObjectHashMap.toString());
                UserDailyPv userDailyPv = new UserDailyPv(stringObjectHashMap);
                switch (after.getString("stat_range")) {
                    case "user_daily":
                        context.output(userDailyOutputTag, userDailyPv);
                        break;
                    case "user_hourly":
                        context.output(userHourlyOutputTag, userDailyPv);
                        break;
                    case "daily":
                        context.output(dailyOutputTag, userDailyPv);
                        break;
                    case "hourly":
                        context.output(hourlyOutputTag, userDailyPv);
                        break;
                }
                stringObjectHashMap.clear();
            }
        });

        process.getSideOutput(userDailyOutputTag)
                .addSink(JdbcSink.sink("insert into s_pv_daily_user_change as d(start_time,end_time,uid,stat_time,op,pv,driver_time) values(?,?,?,?,?,?,?) " +
                                "on conflict (start_time,end_time,uid,stat_time) " +
                                "do update set op='u', pv=EXCLUDED.pv, driver_time=EXCLUDED.driver_time " +
                                "where d.driver_time<=EXCLUDED.driver_time",
                        (JdbcStatementBuilder<UserDailyPv>) (preparedStatement, userDailyPv) -> {
                            preparedStatement.setTimestamp(1, new Timestamp(userDailyPv.getStartTime()-28800000));
                            preparedStatement.setTimestamp(2, new Timestamp(userDailyPv.getEndTime()-28800000));
                            preparedStatement.setString(3, userDailyPv.getUid());
                            preparedStatement.setTimestamp(4, new Timestamp(userDailyPv.getStatTime()-28800000));
                            preparedStatement.setString(5, userDailyPv.getOp());
                            preparedStatement.setInt(6, userDailyPv.getPv());
                            preparedStatement.setTimestamp(7, new Timestamp(userDailyPv.getDriverTime()-28800000));
                        },
                        new JdbcExecutionOptions.Builder().withBatchIntervalMs(2000).withBatchSize(1000).withMaxRetries(5).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:postgresql://localhost:5432/postgres?currentSchema=lab")
                                .withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build()
                ));

        //设置execute
        env.execute("This is a lab");
    }
}
