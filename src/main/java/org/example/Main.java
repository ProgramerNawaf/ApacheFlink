//package org.example;
//
//import org.apache.avro.reflect.ReflectData;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.connector.file.sink.FileSink;
//import org.apache.flink.connector.jdbc.JdbcInputFormat;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.formats.parquet.ParquetWriterFactory;
//import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
//import org.apache.flink.types.Row;
//import org.apache.parquet.avro.AvroParquetWriter;
//import org.apache.parquet.hadoop.ParquetFileWriter;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.flink.formats.parquet.ParquetWriterFactory;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.avro.AvroParquetWriter;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.flink.core.fs.Path;
//import org.apache.avro.reflect.ReflectData;
//import java.sql.Timestamp;
//
//public class Main {
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        JdbcInputFormat jdbcInput = JdbcInputFormat.buildJdbcInputFormat()
//                .setDrivername("com.microsoft.sqlserver.jdbc.SQLServerDriver")
//                .setDBUrl("jdbc:sqlserver://localhost:1433;databaseName=DataQuality;encrypt=false;")
//                .setUsername("sa")
//                .setPassword("123456")
//                .setQuery("SELECT id, Agency_Code, Insetrtimestamp FROM dbo.issuelist ")
//                .setRowTypeInfo(new RowTypeInfo(
//                        Types.INT,
//                        Types.STRING,
//                        Types.SQL_TIMESTAMP
//                ))
//                .finish();
//
//        DataStream<Row> dbStream = env.createInput(jdbcInput);
//
//        DataStream<IssueRecord> recordStream = dbStream.map(row -> new IssueRecord(
//                (int) row.getField(0),
//                (String) row.getField(1),
//                (Timestamp) row.getField(2)
//        ));
////        recordStream.print();
////        env.execute("Print Issue Records");
//
//
////        ParquetWriterFactory<IssueRecord> writerFactory = new ParquetWriterFactory<>(outPath ->
////                AvroParquetWriter.<IssueRecord>builder(outPath)
////                        .withSchema(ReflectData.AllowNull.get().getSchema(IssueRecord.class))
////                        .withCompressionCodec(CompressionCodecName.SNAPPY)
////                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
////                        .build());
//
//        FileSink<IssueRecord> sink = FileSink
//                .forBulkFormat(new Path("file:///C:/flink/parquet_output"), writerFactory)
//                .withBucketAssigner(new InsetrtimestampBucketAssigner())
//                .withOutputFileConfig(
//                        OutputFileConfig.builder()
//                                .withPartPrefix("part")
//                                .withPartSuffix(".parquet")
//                                .build()
//                )
//                .build();
//
//        recordStream.sinkTo(sink);
//
//        env.execute("Write Partitioned Parquet by Insetrtimestamp");
//    }
//}
