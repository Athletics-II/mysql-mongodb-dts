package org.dts.model;

import java.util.Map;

public class BinlogEvent {
    private EventType eventType;
    private String tableName;
    private String primaryKeyValue;
    private Map<String, Object> data; // Data after the event (for INSERT and UPDATE)
    private Map<String, Object> beforeData; // Data before the event (mainly for UPDATE)

    public BinlogEvent(EventType eventType, String tableName, String primaryKeyValue, Map<String, Object> data) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.primaryKeyValue = primaryKeyValue;
        this.data = data;
    }

    public BinlogEvent(EventType eventType, String tableName, String primaryKeyValue, Map<String, Object> data, Map<String, Object> beforeData) {
        this(eventType, tableName, primaryKeyValue, data);
        this.beforeData = beforeData;
    }

    // Getters and Setters
    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    public void setPrimaryKeyValue(String primaryKeyValue) {
        this.primaryKeyValue = primaryKeyValue;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getBeforeData() {
        return beforeData;
    }

    public void setBeforeData(Map<String, Object> beforeData) {
        this.beforeData = beforeData;
    }

    @Override
    public String toString() {
        return "BinlogEvent{" +
                "eventType=" + eventType +
                ", tableName='" + tableName + '\'' +
                ", primaryKeyValue='" + primaryKeyValue + '\'' +
                ", data=" + data +
                ", beforeData=" + beforeData +
                '}';
    }
}
