package com.leqee.etl.sink;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public interface CdcFlushable extends Flushable {
    void flush(BlockingQueue<String> tableBuffer) throws Exception;

    @Override
    default void flush() throws IOException {}
}
