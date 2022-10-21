package api.doc.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author wangguochao
 * @date 2022/10/19
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("10.10.10.90", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");

//        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
//        env2.setRuntimeMode(RuntimeExecutionMode.BATCH);


//        DataStream<Long> someIntegers = env.generateSequence(0, 1000);
//        IterativeStream<Long> iteration = someIntegers.iterate();
//
//        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
//            @Override
//            public Long map(Long aLong) throws Exception {
//                System.out.println(aLong);
//                return aLong - 1;
//            }
//        });
//
//        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long aLong) throws Exception {
//                return (aLong > 0);
//            }
//        });
//
//        iteration.closeWith(stillGreaterThanZero);
//
//        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long aLong) throws Exception {
//                return (aLong <= 0);
//            }
//        });
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word: sentence.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
