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

package com.leqee.etl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.leqee.etl.internal.event.CdcEvent;
import com.leqee.etl.internal.serialisation.CdcEventDebeziumDeserializeSchema;
import com.leqee.etl.sink.CdcSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

public class Application {

	private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {
		MySqlSource<CdcEvent> mySqlSource = MySqlSource.<CdcEvent>builder()
				.hostname("localhost")
				.port(3306)
				.username("root")
				.password("")
				.databaseList("dev_caleb")
				.tableList("dev_caleb.dbz_test")
				.serverTimeZone("Asia/Shanghai")
				.deserializer(new CdcEventDebeziumDeserializeSchema())
				.startupOptions(StartupOptions.latest())
				.includeSchemaChanges(true)
				.build();


		env.setRestartStrategy(RestartStrategies.noRestart());

		env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);

		env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
		.addSink(new CdcSink("local"));
		env.execute("Print MySQL Snapshot + Binlog");
	}
}
