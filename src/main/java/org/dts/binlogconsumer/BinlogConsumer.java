package org.dts.binlogconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.dts.model.BinlogEvent;
import org.dts.model.EventType;
import org.dts.parser.BinlogParser;
import org.dts.rabbitmq.RabbitMQConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class BinlogConsumer {

    private final static String QUEUE_NAME = "dts_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                BinlogEvent event = parseMessage(message);
                BinlogParser parser = new BinlogParser("localhost", "testDb", "testCollection");
                parser.processEvent(event);

            };

            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        }
    }

    private static BinlogEvent parseMessage(String message) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(message);
        JsonNode metadataNode = rootNode.path("metadata");
        JsonNode dataNode = rootNode.path("data");

        EventType eventType = EventType.valueOf(metadataNode.path("operation").asText().toUpperCase());
        String tableName = metadataNode.path("table").asText();
        String primaryKeyValue = extractPrimaryKeyValue(dataNode); // Assuming primary key can be identified

        Map<String, Object> dataMap = convertJsonToMap(dataNode);

        switch (eventType) {
            case INSERT:
            case UPDATE:
                // Assuming 'beforeData' is provided for UPDATE in another part of JSON
                JsonNode beforeDataNode = rootNode.path("beforeData");
                Map<String, Object> beforeDataMap = convertJsonToMap(beforeDataNode);
                return new BinlogEvent(eventType, tableName, primaryKeyValue, dataMap, beforeDataMap);
            case DELETE:
                return new BinlogEvent(eventType, tableName, primaryKeyValue, dataMap);
            default:
                throw new IllegalArgumentException("Unsupported event type: " + eventType);
        }
    }

    private static String extractPrimaryKeyValue(JsonNode dataNode) {
        // Implementation depends on your data structure. You may need to define how to find the primary key.
        return dataNode.path("id").asText(); // Example: Assuming 'id' is the primary key
    }

    private static Map<String, Object> convertJsonToMap(JsonNode jsonNode) {
        Map<String, Object> result = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            result.put(field.getKey(), field.getValue().asText());
        }
        return result;
    }
}
