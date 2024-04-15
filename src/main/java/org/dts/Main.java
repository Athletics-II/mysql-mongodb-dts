package org.dts;

import org.dts.model.BinlogEvent;
import org.dts.model.EventType;
import org.dts.parser.BinlogParser;
import org.dts.rabbitmq.EventConsumer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws IOException, TimeoutException {
        int numThreads = 5;  // Example: 5 threads
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        BinlogParser parser = new BinlogParser("localhost", "testDb", "testCollection");
        BinlogEvent insertEvent = new BinlogEvent(EventType.INSERT, "test", "1", Map.of("name", "John Doe", "age", 30), null);
        BinlogEvent updateEvent = new BinlogEvent(EventType.UPDATE, "test", "1", Map.of("name", "John Doe", "age", 25), Map.of("name", "John Doe", "age", 30));
        BinlogEvent queryEvent = new BinlogEvent(EventType.QUERY, "test", "1", null, null);
        BinlogEvent deleteEvent = new BinlogEvent(EventType.DELETE, "test", "1", null, Map.of("name", "John Doe", "age", 25));

        parser.processEvent(insertEvent);
        parser.processEvent(updateEvent);
        parser.processEvent(queryEvent);
        parser.processEvent(deleteEvent);
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    EventConsumer consumer = new EventConsumer();
                    consumer.startConsumer();
                } catch (Exception e) {
                    System.err.println("Exception in consumer thread: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
    }
}