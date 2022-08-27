package com.leqee.etl.util;

import java.time.Duration;

public class CdcConfiguration {
    public static final Integer RETRY_TIMES_WHEN_BUFFER_FULL = 3;
//            ConfigOptions
//                    .key("buffer.retry-time-when-full")
//                    .intType()
//                    .defaultValue(3)
//                    .withDescription("when table buffer full, it will flush buffer, and retry to put data to buffer");

    public static final Integer FLUSH_MAX_ROWS = 8;
//            ConfigOptions
//                    .key("buffer.flush-max-rows")
//                    .intType()
//                    .defaultValue(100)
//                    .withDescription("when buffer's size more than the number, it will flush buffer");
    public static final Duration FLUSH_INTERVAL = Duration.ofSeconds(8);
//            ConfigOptions
//                    .key("buffer.flush-interval")
//                    .durationType()
//                    .defaultValue(Duration.ofSeconds(3))
//                    .withDescription("flush buffer every specified interval");
    public static final Integer SCHEDULED_EXECUTOR_SIZE = 10;

    public static final String SOURCE_INSTANCE_URL = "jdbc:mysql://localhost:3306";
    public static final String SOURCE_INSTANCE_USER = "root";
    public static final String SOURCE_INSTANCE_PWD = "";
    public static final String TARGET_INSTANCE_URL = "jdbc:mysql://localhost:3306/cdc?useServerPrepStmts=false&rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai";
    public static final String TARGET_INSTANCE_USER = "root";
    public static final String TARGET_INSTANCE_PWD = "";
    public static final String TARGET_INSTANCE_SCHEMA = "cdc";

    public static final Integer EXECUTOR_CORE_POOL_SIZE = 8;
    public static final Integer EXECUTOR_MAXIMUM_POOL_SIZE = 32;
    public static final Long EXECUTOR_THREAD_IDLE_TTL = 60L; //Second
    public static final Integer EXECUTOR_WORK_QUEUE_CAPACITY = 64;

    public static final Integer TABLE_ACTIVE_THREAD_NUM = 4;
}
