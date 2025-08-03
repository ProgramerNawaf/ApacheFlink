package org.example;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.codehaus.jettison.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class CDCReader {

    private static String esc(String input) {
        return input == null ? "NULL" : "'" + input.replace("'", "''") + "'";
    }

    private static String toStr(Object obj) {
        return obj == null ? "NULL" : obj.toString();
    }

    public static void main(String[] args) throws Exception {


        Properties props = new Properties();
//        try (FileInputStream input = new FileInputStream("./src/main/resources/cdc-config.properties")) {
//            props.load(input);
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to load CDC config file: " + "cdc-config.properties", e);
//        }
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1); // CDC source must be single parallelism

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. Enable checkpointing
//        env.enableCheckpointing(10000); // every 10 seconds
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);

        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("useSSL", "false");  // disable SSL for dev
//        props.setProperty("serverTimezone", "UTC");  // ✅ or "Asia/Riyadh"
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("cdc_user")
                .password("2610")
                .databaseList("DataQuality")
                .tableList("DataQuality.Issuelist")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.earliest())
                .serverTimeZone("UTC") // Set server timezone to match MySQL
                .build();


        DataStream<IssueRecord> parsed = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "MySQL CDC Source"
                )
                .filter(json -> {
                    JSONObject obj = new JSONObject(json);
                    return !"r".equals(obj.optString("op", ""));
                })
                .map(json -> {
                    JSONObject obj = new JSONObject(json);
                    System.out.println("Full JSON1: " + obj.toString(2));

                    JSONObject src = obj.getJSONObject("source");
                    JSONObject update = null;
                    IssueRecord record = new IssueRecord();

                    record.SynchTimestamp = new Timestamp(src.getLong("ts_ms")).toString();
                    String op = obj.optString("op", "");
                    switch (op) {
                        case "c": {
                            record.LastOperation = "I";
                            update = obj.getJSONObject("after");
                        } break;
                        case "u": {
                            record.LastOperation = "U";
                            update = obj.getJSONObject("after");
                        } break;
                        case "d": {
                            record.LastOperation = "D";
                            update = obj.getJSONObject("before");
                        } break;
                        default:
                            record.LastOperation = op;  // fallback if unknown
                    }

                    System.out.println(" hello world 2");
                    System.out.println("Full JSON: " + obj.toString(2));

                    record.ID = update.optInt("ID");
                    record.Profiling_ID = update.opt("Profiling_ID") != JSONObject.NULL ? update.getInt("Profiling_ID") : null;
                    record.Execution_ID = update.opt("Execution_ID") != JSONObject.NULL ? update.getInt("Execution_ID") : null;
                    record.Agency_Code = update.optString("Agency_Code", null);
                    record.Dataset_Code = update.optString("Dataset_Code", null);
                    record.Table_Name = update.optString("Table_Name", null);
                    record.Column_Name = update.optString("Column_Name", null);
                    record.Result = update.optString("Result", null);
                    record.SubResult = update.optString("SubResult", null);
                    record.Query = update.optString("Query", null);
                    record.issueTypeID = update.opt("issueTypeID") != JSONObject.NULL ? update.getInt("issueTypeID") : null;
                    record.StatusID = update.opt("StatusID") != JSONObject.NULL ? update.getInt("StatusID") : null;
                    record.PraiortyID = update.opt("PraiortyID") != JSONObject.NULL ? update.getInt("PraiortyID") : null;
                    record.needReinitial = update.opt("needReinitial") != JSONObject.NULL ? update.getBoolean("needReinitial") : null;
                    record.initialstatus = update.opt("initialstatus") != JSONObject.NULL ? update.getInt("initialstatus") : null;
                    record.reinitialdate = update.opt("reinitialdate") != JSONObject.NULL ? update.getString("reinitialdate") : null;
                    record.reportdate = update.opt("reportdate") != JSONObject.NULL ? new Timestamp(update.getLong("reportdate")).toString() : null;
                    record.issueopendate = update.opt("issueopendate") != JSONObject.NULL ? new Timestamp(update.getLong("issueopendate")).toString() : null;
                    record.issueclosedate = update.opt("issueclosedate") != JSONObject.NULL ? new Timestamp(update.getLong("issueclosedate")).toString() : null;
                    record.updatedby = update.opt("updatedby") != JSONObject.NULL ? update.getInt("updatedby") : null;
                    record.insertedby = update.opt("insertedby") != JSONObject.NULL ? update.getInt("insertedby") : null;
                    record.note = update.optString("note", null);
                    record.updatetimestamp = update.opt("synchtimestamp") != JSONObject.NULL ? new Timestamp(update.getLong("synchtimestamp")).toString() : null;
                    record.Insetrtimestamp = update.opt("Insetrtimestamp") != JSONObject.NULL ? new Timestamp(update.getLong("Insetrtimestamp")).toString() : null;

                    return record;
                });

        parsed.addSink(JdbcSink.sink(
                "INSERT INTO dbo.Issuelist4 (" +
                        " Profiling_ID, Execution_ID, Agency_Code, Dataset_Code, Table_Name, Column_Name, Result, SubResult, Query, " +
                        "issueTypeID, StatusID, PraiortyID, needReinitial, initialstatus, reinitialdate, reportdate, issueopendate, issueclosedate, " +
                        "updatedby, insertedby, note, updatetimestamp, Insetrtimestamp,LastOperation,SynchTimestamp,ID) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, r) -> {
                    ps.setObject(1, r.Profiling_ID);
                    ps.setObject(2, r.Execution_ID);
                    ps.setString(3, r.Agency_Code);
                    ps.setString(4, r.Dataset_Code);
                    ps.setString(5, r.Table_Name);
                    ps.setString(6, r.Column_Name);
                    ps.setString(7, r.Result);
                    ps.setString(8, r.SubResult);
                    ps.setString(9, r.Query);
                    ps.setObject(10, r.issueTypeID);
                    ps.setObject(11, r.StatusID);
                    ps.setObject(12, r.PraiortyID);
                    ps.setObject(13, r.needReinitial);
                    ps.setObject(14, r.initialstatus);
                    ps.setString(15, r.reinitialdate);
                    ps.setString(16, r.reportdate);
                    ps.setString(17, r.issueopendate);
                    ps.setString(18, r.issueclosedate);
                    ps.setObject(19, r.updatedby);
                    ps.setObject(20, r.insertedby);
                    ps.setString(21, r.note);
                    ps.setString(22, r.updatetimestamp);
                    ps.setString(23, r.Insetrtimestamp);
                    ps.setString(24, r.LastOperation);
                    ps.setString(25, r.SynchTimestamp);
                    ps.setInt(26, r.ID);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(500)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:sqlserver://localhost:1433;databaseName=DataQuality;encrypt=false")
                        .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .withUsername("sa")
                        .withPassword("123456")
                        .build()
        ));

        env.execute("CDC to SQL Server Sink");
//
//        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
//                .hostname("localhost")
//                .port(5432)
//                .database("datacamp") // e.g. "test_db"
//                .schemaList("public")      // monitor this schema
//                .tableList("public.customer") // full table name
//                .username("cdc_user")
//                .password("12345")
//                .deserializer(new com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema())
////                .startupOptions(StartupOptions.initial()) // or .latest()
//                .build();
//
//        env.addSource(sourceFunction).print();
//
//        env.execute("PostgreSQL CDC Example");
//
//
////        Properties debeziumProps = new Properties();
////        debeziumProps.setProperty("database.encrypt", "false");
//
////        DataStreamSource<String> source = env.addSource(
////                SqlServerSource.<String>builder()
////                        .hostname("localhost")
////                        .port(1433)
////                        .database("DataQuality")
////                        .tableList("dbo.issuelist")
////                        .username("sa")
////                        .password("123456")
////                        .deserializer(new StringDebeziumDeserializationSchema())
////                        .debeziumProperties(debeziumProps)  // Add Debezium properties
////                        .build()
////        );
////
////        source.print(); // or send to sink (Kafka, HDFS, etc.)
////        env.execute("SQL Server CDC");
    }
}
