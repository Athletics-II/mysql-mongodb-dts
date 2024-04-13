package org.dts.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.dts.model.BinlogEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventProducer {
    private static final String EXCHANGE_NAME = "dts_events";
    private static final String ROUTING_KEY = "event_key";

    public void publishEvent(BinlogEvent event) throws IOException, TimeoutException {
        try (Connection connection = RabbitMQConfig.createConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
            ObjectMapper mapper = new ObjectMapper();
            String message = mapper.writeValueAsString(event);
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
