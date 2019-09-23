package com.stillcoolme;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: stillcoolme
 * @date: 2019/8/26 11:12
 * @description:
 *  0. 将pom文件中flink包的provided去除，然后执行main方法
 *  1. 解压netcat1.12.rar
 *  2. 在netcat1.12文件夹内打开cmd，执行ncat -lk 9000，然后输入字符即可（linux平台为nc -lk 9000）
 **/
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建了一个字符串类型的 DataStream
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        DataStream<Tuple2<String, Integer>> wordCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for(String word: s.split("\\s")){
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 将 数据流按照单词字段（即0号索引字段）做分组，这里可以简单地使用 keyBy(int index) 方法
        DataStream<Tuple2<String, Integer>> windowCounts = wordCounts.keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);        // 为每个key每个窗口指定了sum聚合函数

        windowCounts.print().setParallelism(1);
        // 所有算子操作（例如创建源、聚合、打印）只是构建了内部算子操作的图形。
        // 只有在execute()被调用时才会在提交到集群上或本地计算机上执行。
        env.execute("Socket Window WordCount");
    }
}
