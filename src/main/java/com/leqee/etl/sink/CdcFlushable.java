package com.leqee.etl.sink;

import com.leqee.etl.internal.event.CdcEvent;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public interface CdcFlushable extends Flushable {
    void flush(BlockingQueue<CdcEvent> tableBuffer) throws Exception;

    @Override
    default void flush() throws IOException {}
}
