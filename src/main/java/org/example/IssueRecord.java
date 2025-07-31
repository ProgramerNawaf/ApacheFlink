package org.example;
import java.sql.Timestamp;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
public class IssueRecord implements Serializable {
    public int id;
    public String tableName;
    public long insertTimestamp;  // ✅ Use long

    public IssueRecord() {}

    public IssueRecord(int id, String tableName, Timestamp insertTimestamp) {
        this.id = id;
        this.tableName = tableName;
        this.insertTimestamp = insertTimestamp.getTime(); // ✅ convert to epoch millis
    }

    public String getPartitionDate() {
        return java.time.Instant.ofEpochMilli(insertTimestamp)
                .atZone(java.time.ZoneId.systemDefault())
                .toLocalDate()
                .toString();
    }
}

