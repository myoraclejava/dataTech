/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ljw.dwd;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class EtlF_ord {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		env.getCheckpointConfig().setCheckpointTimeout(3600000);
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStateBackend(new HashMapStateBackend());
		env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp");
		StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
		tabEnv.getConfig().getConfiguration().setString(PipelineOptions.NAME,"sql job");
		
		String createCdcOrder = "CREATE TABLE cdcOrder(" +
				"\norder_id int," +
				"\norder_no varchar," +
				"\nuser_id int," +
				"\nprod_id int," +
				"\ndelv_id int," +
				"\norder_time timestamp," +
				"\norder_amt decimal(18,2)," +
				"\nupdate_time timestamp," +
				"\nproc_time AS PROCTIME()" +
				//"\nprimary key(order_id) not enforced" +
				"\n) with (" +
				"\n'connector'='postgres-cdc'," +
				"\n'hostname'='hadoop01'," +
				"\n'port'='5432'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'database-name'='postgres'," +
				"\n'table-name'='order'," +
				"\n'schema-name'='src'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'slot.name'='flink12'," +
				"\n'decoding.plugin.name' = 'pgoutput'," +
				"\n'debezium.snapshot.mode' = 'initial'," +
				"\n'changelog-mode' = 'all'" +
				"\n)";

		String createJdbcUser = "CREATE TABLE jdbcUser(" +
				"\nuser_id int," +
				"\nuser_name varchar" +
				//"\nprimary key(user_id) not enforced" +
				"\n) with (" +
				"\n'connector'='jdbc'," +
				"\n'url'='jdbc:postgresql://hadoop01:5432/postgres?currentSchema=dim'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'table-name'='d_user'" +
				"\n)";

		String createJdbcF_ord = "CREATE TABLE jdbcF_ord(" +
				"\nord_id int," +
				"\nord_no varchar," +
				"\nuser_id int," +
				"\nuser_name varchar," +
				"\nprod_id int," +
				"\nprod_name varchar," +
				"\nprod_type varchar," +
				"\ndelv_id int," +
				"\nord_qty int," +
				"\nord_amt decimal(18,2)," +
				"\nuser_qty int," +
				"\nord_date date," +
				"\nord_time timestamp," +
				"\nprimary key(ord_id) not enforced" +
				"\n) with (" +
				"\n'connector'='jdbc'," +
				"\n'url'='jdbc:postgresql://hadoop01:5432/postgres?currentSchema=dwd'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'table-name'='f_ord'," +
				"\n'sink.buffer-flush.max-rows'='1'," +
				"\n'sink.buffer-flush.interval'='0'" +
				"\n)";

		String createCdcUser = "CREATE TABLE cdcUser(" +
				"\nuser_id int," +
				"\nuser_name varchar," +
				"\nproc_time AS PROCTIME()," +
				"\nprimary key(user_id) not enforced" +
				"\n) with (" +
				"\n'connector'='postgres-cdc'," +
				"\n'hostname'='hadoop01'," +
				"\n'port'='5432'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'database-name'='postgres'," +
				"\n'table-name'='d_user'," +
				"\n'schema-name'='dim'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'slot.name'='flink5'," +
				"\n'decoding.plugin.name' = 'pgoutput'," +
				"\n'debezium.snapshot.mode' = 'initial'," +
				"\n'changelog-mode' = 'upsert'" +
				"\n)";

		String createJdbcF_ord2 = "CREATE TABLE jdbcF_ord2(" +
				"\nord_id int," +
				"\nuser_id int," +
				"\nuser_name varchar," +
				"\nprimary key(ord_id) not enforced" +
				"\n) with (" +
				"\n'connector'='jdbc'," +
				"\n'url'='jdbc:postgresql://hadoop01:5432/postgres?currentSchema=dwd'," +
				"\n'username'='postgres'," +
				"\n'password'='postgres'," +
				"\n'table-name'='f_ord'" +
				"\n)";

		String insertJdbcF_ord = "INSERT INTO jdbcF_ord(ord_id,ord_no,user_id,user_name,prod_id,ord_qty,ord_amt,user_qty,ord_date,ord_time,delv_id)\n" +
				"\nSELECT order_id,order_no,o.user_id,u.user_name,prod_id,1 as ord_qty,order_amt,o.user_id,cast(order_time as date) as ord_date,order_time,delv_id\n" +
				"\nFROM cdcOrder o" +
				"\nLEFT JOIN jdbcUser FOR SYSTEM_TIME AS OF o.proc_time u" +
				"\nON o.user_id = u.user_id";

		String insertJdbcF_ord2 = "INSERT INTO jdbcF_ord2(ord_id,user_id,user_name)" +
				"\nSELECT o.ord_id,u.user_id,u.user_name" +
				"\nFROM cdcUser u" +
				"\nJOIN jdbcF_ord2 FOR SYSTEM_TIME AS OF u.proc_time o " +
				"ON u.user_id = o.user_id";

		tabEnv.executeSql(createCdcOrder);
		tabEnv.executeSql("select * from cdcOrder").print();
		tabEnv.executeSql(createJdbcUser);
		tabEnv.executeSql(createJdbcF_ord);
		tabEnv.executeSql(createCdcUser);
		tabEnv.executeSql(createJdbcF_ord2);
		StreamStatementSet stmt = tabEnv.createStatementSet();
		//stmt.addInsertSql(insertJdbcF_ord);
		//stmt.addInsertSql(insertJdbcF_ord2);
		stmt.execute();

	}
}
