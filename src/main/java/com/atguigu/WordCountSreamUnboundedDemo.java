package com.atguigu;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountSreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost",7777);

        // 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((Tuple2<String, Integer> value) -> {
                    return value.f0;
                })
                .sum(1);

        // 输出
        sum.print();

        // 执行
        env.execute();

    }
}
