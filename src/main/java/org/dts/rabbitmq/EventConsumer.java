package org.dts.rabbitmq;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class EventConsumer {
    private static final String QUEUE_NAME = "dts_queue";

    public void startConsumer() throws IOException, TimeoutException {
        Connection connection = RabbitMQConfig.createConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, "dts_events", "event_key");

        ExecutorService executor = Executors.newFixedThreadPool(10); // Adjust the number of threads

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            Runnable worker = () -> {
                try {
                    processMessage(delivery);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            };
            executor.submit(worker);
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
    }

    private void processMessage(Delivery delivery) throws UnsupportedEncodingException {
        String message = new String(delivery.getBody(), "UTF-8");
        // Deserialize and process message
        System.out.println("Processed message: " + message);
    }
}
