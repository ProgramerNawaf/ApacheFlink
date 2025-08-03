//package org.example;
//
//import org.apache.flink.core.io.SimpleVersionedSerializer;
//import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
//import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
//
//public class InsetrtimestampBucketAssigner implements BucketAssigner<IssueRecord, String> {
//
////    @Override
////    public String getBucketId(IssueRecord record, Context context) {
////        return "Insetrtimestamp=" + record.getPartitionDate();
////    }
//
//    @Override
//    public SimpleVersionedSerializer<String> getSerializer() {
//        return SimpleVersionedStringSerializer.INSTANCE;
//    }
//}
