package org.example;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class CDCReader {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("database.encrypt", "false");

        DataStreamSource<String> source = env.addSource(
                SqlServerSource.<String>builder()
                        .hostname("localhost")
                        .port(1433)
                        .database("DataQuality")
                        .tableList("dbo.issuelist")
                        .username("sa")
                        .password("123456")
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .debeziumProperties(debeziumProps)  // Add Debezium properties
                        .build()
        );

        source.print(); // or send to sink (Kafka, HDFS, etc.)
        env.execute("SQL Server CDC");
    }
}
