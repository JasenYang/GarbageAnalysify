package com.fk;

import com.alibaba.tianchi.garbage_image_util.IdLabel;
import com.alibaba.tianchi.garbage_image_util.ImageClassSink;
import com.alibaba.tianchi.garbage_image_util.ImageDirSource;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Main {
    public static void main(String[] args) {
        /**
         * export IMAGE_MODEL_PACKAGE_PATH=/home/gavin/workspace/tianchi/saved_model.tar.gz
         * export IMAGE_INPUT_PATH=/home/gavin/workspace/ApachFlink/src/main/resources/inference
         * export IMAGE_MODEL_PATH=/home/gavin/workspace/tianchi/ApachFlink/saved_model
         */
        StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkEnv.setParallelism(1);
        ImageDirSource source = new ImageDirSource();
        DataStreamSink<IdLabel> result = flinkEnv.addSource(source).setParallelism(1)
                .flatMap(new ProcessingFlatMap())
                .setParallelism(2)
                .flatMap(new UpdateDebugFlatMap(System.getenv("IMAGE_INPUT_PATH"), System.getenv("IMAGE_MODEL_PACKAGE_PATH")))
                .setParallelism(3)
                .addSink(new ImageClassSink())
                .setParallelism(1);
        try {
            flinkEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


