package org.dts.parser;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.dts.model.BinlogEvent;

public class BinlogParser {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public BinlogParser(String mongoDbHost, String dbName, String collectionName) {
        this.mongoClient = MongoClients.create(mongoDbHost);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
    }

    public void processEvent(BinlogEvent event) {
        switch (event.getEventType()) {
            case INSERT:
                handleInsert(event);
                break;
            case UPDATE:
                handleUpdate(event);
                break;
            case DELETE:
                handleDelete(event);
                break;
            default:
                System.out.println("Unsupported event type.");
                break;
        }
    }

    private void handleInsert(BinlogEvent event) {
        Document doc = new Document(event.getData());
        collection.insertOne(doc);
        System.out.println("Inserted document: " + doc.toJson());
    }

    private void handleUpdate(BinlogEvent event) {
        Document query = new Document("id", event.getPrimaryKeyValue());
        Document update = new Document("$set", new Document(event.getData()));
        collection.updateOne(query, update);
        System.out.println("Updated document with ID: " + event.getPrimaryKeyValue());
    }

    private void handleDelete(BinlogEvent event) {
        Document query = new Document("id", event.getPrimaryKeyValue());
        collection.deleteOne(query);
        System.out.println("Deleted document with ID: " + event.getPrimaryKeyValue());
    }
}
