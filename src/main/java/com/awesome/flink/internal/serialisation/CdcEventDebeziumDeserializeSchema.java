package com.awesome.flink.internal.serialisation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.awesome.flink.internal.event.DDLEvent;
import com.awesome.flink.internal.event.CdcEvent;
import com.awesome.flink.internal.event.DMLEvent;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

public class CdcEventDebeziumDeserializeSchema implements DebeziumDeserializationSchema<CdcEvent> {
    private static final long serialVersionUID = 3L;

    @Override
    public void deserialize(SourceRecord record, Collector<CdcEvent> out) throws Exception {
        CdcEvent event;
        if (CdcEvent.isDdlEvent(record)) {
            event = new DDLEvent.Builder(record)
                    .extractDdl()
                    .build();
        } else {
            event = new DMLEvent
                    .Builder(record)
                    .extractRow()
                    .extractOp()
                    .build();
        }
        out.collect(event);
    }

    @Override
    public TypeInformation<CdcEvent> getProducedType() {
        return TypeInformation.of(CdcEvent.class);
    }
}
