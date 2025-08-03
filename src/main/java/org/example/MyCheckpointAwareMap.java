//package org.example;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//import org.apache.flink.runtime.state.CheckpointListener;
//
//public class MyCheckpointAwareMap implements MapFunction<String, String>, CheckpointListener {
//
//    @Override
//    public String map(String value) {
//        return "Mapped: " + value;
//    }
//
//    @Override
//    public void notifyCheckpointComplete(long checkpointId) {
//        System.out.println("✅ Checkpoint complete: " + checkpointId);
//    }
//
//    @Override
//    public void notifyCheckpointAborted(long checkpointId) {
//        System.out.println("❌ Checkpoint aborted: " + checkpointId);
//    }
//}
