package com.fk;

import com.alibaba.tianchi.garbage_image_util.ImageData;
import com.intel.analytics.zoo.pipeline.inference.JTensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class ProcessingFlatMap extends RichFlatMapFunction<ImageData, Tuple2<String, JTensor>> {
    public float[] convertToFloat(byte[] buffer) {
        float[] fArr = new float[buffer.length];  // 4 bytes per float
        for (int i = 0; i < fArr.length; i++)
        {
            fArr[i] = buffer[i];
        }
        return fArr;
    }

    public float[] Normalization(float[] floats, int l) {
        // 把所有的图片长度, 归一化成6000
        int lth = floats.length;
        if (lth == 6000) {
            return floats;
        } else if(lth > 6000) {
            float[] newf = new float[6000];
            int j = 0;
            for (int i=0; i < 2000; i ++, j++) {
                newf[j] = floats[i];
            }
            for (int i= lth/2 - 1000; i < lth/2 + 1000; i++, j++) {
                newf[j] = floats[i];
            }
            for (int i = lth - 2000; i < lth; i++, j++) {
                newf[j] = floats[i];
            }
            return newf;
        } else {
            float[] newf = new float[6000];
            int j = 0;
            for (int i=0; i < lth/3 ; i ++, j++ ) {
                newf[i] = floats[j];
            }
            for (int i = lth/3; i < (6000 - lth)/2 + lth/3; i++) {
                newf[i] = 0;
            }
            for (int i = (6000 - lth)/2 + lth/3; i < (6000 - lth)/2 + lth*2/3; i ++, j++) {
                newf[i] = floats[j];
            }
            for (int i = (6000 - lth)/2 + lth*2/3; i < 6000 - lth/3; i ++) {
                newf[i] = 0;
            }
            for (int i=6000 - lth/3; i < 6000; i ++, j++) {
                newf[i] = floats[j];
            }
            return newf;
        }
    }

    @Override
    public void flatMap(ImageData imageData, Collector<Tuple2<String, JTensor>> collector) throws Exception {
        JTensor jTensor = new JTensor();
        int[] shape = {1, 224, 224, 3};
        jTensor.setShape(shape);
        jTensor.setData(convertToFloat(imageData.getImage()));
        collector.collect(new Tuple2<>(imageData.getId(), jTensor));
    }
}
