package flink.dataStreamApi.windowedStream;


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class WindowedStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ToWindowEvent> source = env.addSource(new RichSourceFunction<ToWindowEvent>() {
            private boolean run;

            @Override
            public void run(SourceContext<ToWindowEvent> sourceContext) throws Exception {
                while (run) {

                    sourceContext.collect(ToWindowEvent.builder().country(1).trade(23L).build());
                    TimeUnit.SECONDS.sleep(2);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        });

        KeyedStream<ToWindowEvent, Integer> keyedStream = source.keyBy(new KeySelector<ToWindowEvent, Integer>() {
            @Override
            public Integer getKey(ToWindowEvent s) throws Exception {
                return s.getCountry();
            }
        });

        WindowedStream<ToWindowEvent, Integer, Window> windowedStream = keyedStream.window(new WindowAssigner<ToWindowEvent, Window>() {
            @Override
            public Collection<Window> assignWindows(ToWindowEvent toWindowEvent, long l, WindowAssignerContext windowAssignerContext) {
                return null;
            }

            @Override
            public Trigger<ToWindowEvent, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
                return null;
            }

            @Override
            public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public boolean isEventTime() {
                return false;
            }
        });


        env.execute();
    }



}
