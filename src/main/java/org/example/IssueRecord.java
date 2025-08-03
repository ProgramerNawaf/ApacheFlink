package org.example;
import java.sql.Timestamp;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
public class IssueRecord implements Serializable {
    public int ID;
    public Integer Profiling_ID;
    public Integer Execution_ID;
    public String Agency_Code;
    public String Dataset_Code;
    public String Table_Name;
    public String Column_Name;
    public String Result;
    public String SubResult;
    public String Query;
    public Integer issueTypeID;
    public Integer StatusID;
    public Integer PraiortyID;
    public Boolean needReinitial;
    public Integer initialstatus;
    public String reinitialdate;
    public String reportdate;
    public String issueopendate;
    public String issueclosedate;
    public Integer updatedby;
    public Integer insertedby;
    public String note;
    public String updatetimestamp;
    public String Insetrtimestamp;  // ✅ Use long

    public String LastOperation;  // ✅ Use long

    public String SynchTimestamp;  // ✅ Use long

    public IssueRecord() {}

//    public IssueRecord(int id, String tableName, Timestamp insertTimestamp) {
//        this.id = id;
//        this.tableName = tableName;
//        this.insertTimestamp = insertTimestamp.getTime(); // ✅ convert to epoch millis
//    }
//
//    public String getPartitionDate() {
//        return java.time.Instant.ofEpochMilli(this.insertTimestamp)
//                .atZone(java.time.ZoneId.systemDefault())
//                .toLocalDate()
//                .toString();
//    }
}

