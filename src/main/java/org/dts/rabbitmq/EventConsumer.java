package org.dts.rabbitmq;

import com.rabbitmq.client.*;
import org.dts.parser.BinlogParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dts.model.BinlogEvent;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventConsumer {
    private final static String QUEUE_NAME = "dts_queue";

    public void startConsumer() throws IOException, TimeoutException {
        Connection connection = RabbitMQConfig.createConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, "dts_events", "event_key");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            ObjectMapper mapper = new ObjectMapper();
            BinlogEvent event = mapper.readValue(message, BinlogEvent.class);
            BinlogParser parser = new BinlogParser("localhost", "testDb", "testCollection");
            parser.processEvent(event);
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
    }
}
