package com.leqee.etl.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.leqee.etl.internal.dialect.MySqlDialect;
import com.leqee.etl.internal.event.CdcEvent;
import com.leqee.etl.util.CdcConfiguration;
import com.leqee.etl.util.DingDing;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class CdcSink extends RichSinkFunction<CdcEvent> implements CheckpointedFunction, CdcFlushable {
    /**
     * Prefer {@link java.util.concurrent.LinkedBlockingQueue} to {@link java.util.concurrent.ArrayBlockingQueue},
     * because LinkedBlockingDeque does not need to initial capacity and has higher throughput.
     * Each {@link java.util.concurrent.BlockingQueue} buffers data of one table
     */
    private Map<String, BlockingQueue<String>> buffer;

    /**
     * use for spill flush
     */
    private ThreadPoolExecutor threadPool;

    /**
     * use for scheduled flush
     */
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;

    /**
     * use for synchronization between threads of the same table
     */
    private Map<String, Lock> tableLocks;
    private Map<String, AtomicInteger> tableActiveThreadCounters;

    /**
     * db connection
     */
    private Connection sourceConnection;
    private Connection targetConnection;

    /**
     * identifier of source db instance
     */
    private final String instance;

    public CdcSink(String instance) throws Exception {
        this.instance = instance;
    }

    private Connection getConnection(String url, String user, String pwd) throws Exception {
        return DriverManager.getConnection(url, user, pwd);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.buffer = new HashMap<>();
        this.tableLocks = new HashMap<>();
        this.tableActiveThreadCounters = new HashMap<>();
        this.threadPool = new ThreadPoolExecutor(
                CdcConfiguration.EXECUTOR_CORE_POOL_SIZE,
                CdcConfiguration.EXECUTOR_MAXIMUM_POOL_SIZE,
                CdcConfiguration.EXECUTOR_THREAD_IDLE_TTL,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(CdcConfiguration.EXECUTOR_WORK_QUEUE_CAPACITY),
                new ThreadPoolExecutor.AbortPolicy() // if thread pool is full, use subtask thread flush data
        );
        this.scheduler =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("scheduled-flush task submitter"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> buffer.forEach((identifier, buffer) -> {
                            if (buffer.size() > 0) {
                                tryLockAndFlush(identifier, buffer);
                            }
                        }),
                        CdcConfiguration.FLUSH_INTERVAL.getSeconds(),
                        CdcConfiguration.FLUSH_INTERVAL.getSeconds(),
                        TimeUnit.SECONDS);

        this.sourceConnection = getConnection(
                CdcConfiguration.SOURCE_INSTANCE_URL,
                CdcConfiguration.SOURCE_INSTANCE_USER,
                CdcConfiguration.SOURCE_INSTANCE_PWD
        );
        this.targetConnection = getConnection(
                CdcConfiguration.TARGET_INSTANCE_URL,
                CdcConfiguration.TARGET_INSTANCE_USER,
                CdcConfiguration.TARGET_INSTANCE_PWD
        );
    }

    private void submitToThreadPool(String identifier, BlockingQueue<String> buffer) {
        AtomicInteger counter = getTableActiveThreadCounter(identifier);
        if (counter.get() <= CdcConfiguration.TABLE_ACTIVE_THREAD_NUM) {
            // why limit the active thread num of the same table,
            // because once meet some long-time ddl operation, scheduled-flush
            // will submit to many thread then full the thread pool
            counter.incrementAndGet();
            threadPool.submit(() -> tryLockAndFlush(identifier, buffer));
        }
    }

    private void tryLockAndFlush(String identifier, BlockingQueue<String> buffer) {
        Lock lock = getLock(identifier);
        lock.lock();
        flush(buffer);
        lock.unlock();
        getTableActiveThreadCounter(identifier).decrementAndGet();
    }

    @Override
    public void flush(BlockingQueue<String> tableBuffer) {
        if (tableBuffer.size() == 0) {
            return;
        }

        List<String> batch = new ArrayList<>();
        tableBuffer.drainTo(batch);

        try {
            Statement statement = targetConnection.createStatement();

            batch.forEach(sql -> {
                try {
                    statement.addBatch(sql);
                } catch (SQLException throwable) {
                    throwable.printStackTrace();
                }
            });

            statement.executeBatch();
            statement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void invoke(CdcEvent value, Context context) throws Exception {
        String identifier = value.getIdentifier();
        BlockingQueue<String> tableBuffer = getBufferQueue(identifier);

        writeToBuffer(tableBuffer, value);

        if (tableBuffer.size() >= CdcConfiguration.FLUSH_MAX_ROWS) {
            // spill flush
            submitToThreadPool(identifier, tableBuffer);
        }
    }

    @Override
    public void close() throws Exception {
        shutdownSchedulerAndAllFutures()
                .shutdownThreadPool()
                .flushAllLeftBuffer()
                .closeDb();
    }

    private CdcSink shutdownSchedulerAndAllFutures() {
        scheduledFuture.cancel(false);
        scheduler.shutdown();
        return this;
    }

    private CdcSink shutdownThreadPool() {
        threadPool.shutdown();
        return this;
    }

    private CdcSink flushAllLeftBuffer() {
        buffer.forEach(this::tryLockAndFlush);
        return this;
    }

    private void closeDb() throws Exception {
        sourceConnection.close();
        targetConnection.commit();
        targetConnection.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    private void writeToBuffer(BlockingQueue<String> tableBuffer, CdcEvent value) {
        tableBuffer.add(value.getExecutableSql(instance));
    }

    private AtomicInteger getTableActiveThreadCounter(String identifier) {
        return getOrCreateCounter(identifier);
    }

    private AtomicInteger getOrCreateCounter(String identifier) {
        AtomicInteger counter = tableActiveThreadCounters.get(identifier);

        if (counter == null) {
            AtomicInteger newCounter = new AtomicInteger(0);
            tableActiveThreadCounters.put(identifier, newCounter);
            return newCounter;
        } else {
            return counter;
        }
    }

    private BlockingQueue<String> getBufferQueue(String identifier) throws Exception {
        return getOrCreateBufferQueue(identifier);
    }

    private BlockingQueue<String> getOrCreateBufferQueue(String identifier) throws Exception {
        BlockingQueue<String> bufferQueue = buffer.get(identifier);

        if (bufferQueue == null) {
            BlockingQueue<String> newBufferQueue = new LinkedBlockingQueue<>();
            buffer.put(identifier, newBufferQueue);

            // If sink internal does not hold table buffer queue,
            // that means this is the first arrival data from source table,
            // so should check the target table weather exists and create it if it does not
            if (!isTableExist(identifier)) {
                createTargetTable(fetchOriginalSourceDdl(identifier), identifier);
            }

            // Similarly, should check table lock weather is available
            if (!isLockExist(identifier)) {
                addLock(identifier);
            }

            // why target-table-exist-check and table-lock-check are in this method,
            // because data flush and multi-thread operation are all bases on buffer queue,
            // so in this method, should prepare all pre-condition, i.e target table and lock
            return newBufferQueue;
        } else {
            return bufferQueue;
        }
    }

    private Lock getLock(String identifier) {
        // cause `getOrCreateBufferQueue` method, this method definitely return not null
        return tableLocks.get(identifier);
    }

    private Boolean isLockExist(String identifier) {
        return tableLocks.containsKey(identifier);
    }

    private void addLock(String identifier) {
        tableLocks.put(identifier, new ReentrantLock());
    }

    private Boolean isTableExist(String identifier) throws Exception {
        DatabaseMetaData metaData = targetConnection.getMetaData();

        final String[] type = {"TABLE"};

        ResultSet resultSet = metaData.getTables(null,
                CdcConfiguration.TARGET_INSTANCE_SCHEMA,
                String.join("_", instance, identifier.replaceAll("\\.", "_")),
                type);

        Boolean isExist = resultSet.next();
        resultSet.close();

        return isExist;
    }

    private String fetchOriginalSourceDdl(String identifier) throws Exception {
        ResultSet resultSet = sourceConnection.createStatement().executeQuery(String.format("SHOW CREATE TABLE %s", identifier));
        resultSet.next();
        String ddl = resultSet.getString("Create Table");
        resultSet.close();
        return ddl;
    }

    private void createTargetTable(String originalDdl, String identifier) {
        String targetTableName = String.join("_", instance, identifier.replaceAll("\\.", "_"));
        String targetTableDdl = originalDdl.replaceAll("(?<=CREATE TABLE )`\\w+`",
                String.join(".",
                        MySqlDialect.quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                        MySqlDialect.quoteIdentifier(targetTableName)
                        ));
        String isDelColumn = MySqlDialect.isDelColumn(targetTableName);
        String etlTimeColumn = MySqlDialect.etlTimeColumn(targetTableName);
        String isDelIndex = MySqlDialect.isDelIndex(targetTableName);
        String etlTimeIndex = MySqlDialect.etlTimeIndex(targetTableName);
        // `try catch` is fucking so ugly
        try {
            Statement statement = targetConnection.createStatement();
            statement.addBatch(targetTableDdl);
            statement.addBatch(isDelColumn);
            statement.addBatch(etlTimeColumn);
            statement.addBatch(isDelIndex);
            statement.addBatch(etlTimeIndex);

            statement.executeBatch();
            statement.close();
        } catch (SQLException throwable) {
            String message = String.format("fail to create target table, the source table ddl is \n%s\n the target table ddl is \n%s\n",
                    originalDdl,
                    targetTableDdl);
            DingDing.sendMessage(message);
            throwable.printStackTrace();
        }
    }
}
