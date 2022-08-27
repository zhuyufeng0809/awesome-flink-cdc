package com.leqee.etl.internal.partition;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Partitioner;

public abstract class RichPartitioner<K> extends AbstractRichFunction implements Partitioner<K> {

    private static final long serialVersionUID = 1L;

    public abstract int partition(K key, int numPartitions);
}
