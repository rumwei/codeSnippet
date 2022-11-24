import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class Temp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataSource = env.addSource(new RichSourceFunction<String>() {
            private boolean run;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect("");
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        });

        BroadcastStream<Long> configSource = env.addSource(new RichSourceFunction<Long>() {
            private boolean run;

            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(5);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        }).setParallelism(1).broadcast(new MapStateDescriptor<String, Long>("config", String.class, Long.class));

        dataSource.connect(configSource).process(new BroadcastProcessFunction<String, Long, Void>() {
            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Void> collector) throws Exception {

            }

            @Override
            public void processBroadcastElement(Long aLong, Context context, Collector<Void> collector) throws Exception {

            }
        });

        env.execute();
    }


}
