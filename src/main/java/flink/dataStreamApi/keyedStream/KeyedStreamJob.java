package flink.dataStreamApi.keyedStream;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * ============================================
 * ================ Received ==================
 * ============================================
 */
@Slf4j
public class KeyedStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        log.info("start...");

        DataStreamSource<Integer> source = env.addSource(new RichSourceFunction<Integer>() {
            private boolean run = true;
            private int count = 0;

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (run) {
                    sourceContext.collect(count++);
                    TimeUnit.SECONDS.sleep(2);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        }).setParallelism(1);

        KeyedStream<Integer, KeyObj> keyedStream = source.keyBy(new KeySelector<Integer, KeyObj>() {
            @Override
            public KeyObj getKey(Integer integer) throws Exception {
                return new KeyObj(integer, integer % 10);
            }
        });

        keyedStream.process(new KeyedProcessFunction<KeyObj, Integer, Void>() {
            @Override
            public void processElement(Integer integer, Context context, Collector<Void> collector) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                log.info("This is in task{"  + indexOfThisSubtask + "}-key{" + context.getCurrentKey().type + "}-" + "element flow in task is " + integer);
            }
        }).setParallelism(2);

        env.execute();
    }

    @AllArgsConstructor
    public static class KeyObj {
        public int count;
        public int type; //0~10代表10个种类

//        @Override
//        public int hashCode() {
//            return type;
//        }
    }
}
