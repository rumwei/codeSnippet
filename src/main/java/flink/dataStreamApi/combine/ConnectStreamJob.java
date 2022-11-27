package flink.dataStreamApi.combine;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class ConnectStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> strSource = env.addSource(new RichSourceFunction<String>() {
            private boolean run = true;

            @Override
            public void run(SourceFunction.SourceContext<String> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(RandomStringUtils.random(4));
                    TimeUnit.SECONDS.sleep(3);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        });

        DataStreamSource<Integer> integerSource = env.addSource(new RichSourceFunction<Integer>() {
            private boolean run = true;
            private Integer ele = 0;

            @Override
            public void run(SourceFunction.SourceContext<Integer> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(ele++);
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        });
        ConnectedStreams<Integer, String> connectedStreams = integerSource.connect(strSource);
        connectedStreams
//                .flatMap()
//                .map()
                .process(new CoProcessFunction<Integer, String, Void>() {
                    @Override
                    public void processElement1(Integer integer, Context context, Collector<Void> collector) throws Exception {

                    }

                    @Override
                    public void processElement2(String s, Context context, Collector<Void> collector) throws Exception {

                    }
                });


        env.execute();
    }
}
