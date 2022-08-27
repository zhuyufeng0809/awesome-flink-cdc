package com.leqee.etl.internal.partition;

import java.util.Optional;

public class MysqlRecordPartitioner extends RichPartitioner<Optional<String>>{

    private static final long serialVersionUID = 1L;

    @Override
    public int partition(Optional<String> key, int numPartitions) {
        //This means that the table has no key.
        //Have to use one subTask to process.
        return key.map(this::computePartition).orElse(0);
    }

    private int computePartition(String key) {
        return getHash(key) % getParallelism();
    }

    private int getHash(String key) {
        return key.hashCode();
    }

    private int getParallelism() {
        //should get sink parallelism
        return getRuntimeContext().getExecutionConfig().getParallelism();
    }
}
