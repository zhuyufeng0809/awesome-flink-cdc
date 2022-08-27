package com.leqee.etl.internal.partition;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.functions.KeySelector;

public abstract class RichKeySelector<IN, KEY> extends AbstractRichFunction implements KeySelector<IN, KEY> {

    private static final long serialVersionUID = 1L;

    @Override
    public abstract KEY getKey(IN value) throws Exception;
}
