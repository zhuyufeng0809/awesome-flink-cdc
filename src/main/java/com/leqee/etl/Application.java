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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.leqee.etl.util.CdcConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Application {

	private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	private final Connection targetConnection;

	public Application() {
		this.targetConnection = getConnection(
		);
	}

	private Connection getConnection() {
		try {
			return DriverManager.getConnection(CdcConfiguration.TARGET_INSTANCE_URL, CdcConfiguration.TARGET_INSTANCE_USER, CdcConfiguration.TARGET_INSTANCE_PWD);
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}

		return null;
	}

	private void test() {
		String sql = "insert into dev_caleb.dbz_test(new_column) values(?)";
		String sql2 = "insert into dev_caleb.dbz_test(my_column, new_column) values(?, ?)";
		try {
			PreparedStatement statement = targetConnection.prepareStatement(sql);
			statement.setObject(1, 20);
			statement.addBatch();
			statement.addBatch("alter table dev_caleb.`dbz_test` modify `my_column` varchar(100) DEFAULT NULL comment 'test commentsssss'");


			statement.addBatch(sql2);

			statement.setObject(1, 30);
			statement.executeBatch();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}


	}

	public static void main(String[] args) throws Exception {
		Application application = new Application();

		application.test();
//		MySqlSource<CdcEvent> mySqlSource = MySqlSource.<CdcEvent>builder()
//				.hostname("localhost")
//				.port(3306)
//				.username("root")
//				.databaseList("dev_caleb")
//				.tableList("dev_caleb.dbz_test")
//				.serverTimeZone("Asia/Shanghai")
//				.deserializer(new CdcEventDebeziumDeserializeSchema())
//				.startupOptions(StartupOptions.latest())
//				.includeSchemaChanges(true)
//				.build();
//
//
//		env.setRestartStrategy(RestartStrategies.noRestart());
//
//		env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//
//		env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
//		.addSink(new CdcSink("local"));
//		env.execute("Print MySQL Snapshot + Binlog");
	}
}
