package org.dts;

import org.dts.binlogconsumer.BinlogConsumer;
import org.dts.model.BinlogEvent;
import org.dts.model.EventType;
import org.dts.parser.BinlogParser;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws IOException, TimeoutException {

        BinlogParser parser = new BinlogParser("localhost", "testDb", "testCollection");
        BinlogEvent insertEvent = new BinlogEvent(EventType.INSERT, "test", "1", Map.of("name", "John Doe", "age", 30));
        BinlogEvent updateEvent = new BinlogEvent(EventType.UPDATE, "test", "1", Map.of("name", "John Doe", "age", 25), Map.of("name", "John Doe", "age", 30));
        BinlogEvent deleteEvent = new BinlogEvent(EventType.DELETE, "test", "1", null);

        parser.processEvent(insertEvent);
        parser.processEvent(updateEvent);
        parser.processEvent(deleteEvent);
        BinlogConsumer consumer = new BinlogConsumer();

        //TODO: result stats: walltime, throughput, success rate, 95th, 99th

    }
}