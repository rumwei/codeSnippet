package flink.dataStreamApi.combine;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * ============================================
 * ================ Received ==================
 * ============================================
 */
@Slf4j
public class BroadcastKeyedStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataSource = env.addSource(new RichSourceFunction<String>() {
            private boolean run = true;
            private int i = 100000;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(i++ + "");
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        }).setParallelism(1);

        BroadcastStream<Long> configSource = env.addSource(new RichSourceFunction<Long>() {
            private boolean run = true;
            private long config = 0;

            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(config);
                    config += 5;
                    TimeUnit.SECONDS.sleep(5);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        }).setParallelism(1).broadcast(new MapStateDescriptor<String, Long>("config", String.class, Long.class));

        dataSource.keyBy((KeySelector<String, Integer>) s -> (Integer.parseInt(s) % 10)).connect(configSource).process(new KeyedBroadcastProcessFunction<Integer, String, Long, Void>() {
            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Void> collector) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                Integer key = readOnlyContext.getCurrentKey();
                log.info("[Filter]This is in task{{}}-key{{}}-dataElement flow in task is {{}}", indexOfThisSubtask, key, s);
            }

            @Override
            public void processBroadcastElement(Long aLong, Context context, Collector<Void> collector) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                log.info("[Filter]This is in task{{}}-configElement flow in task is {{}}", indexOfThisSubtask, aLong);
            }
        }).setParallelism(2);

        env.execute();
    }
}
