package flink.dataStreamApi.windowedStream;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * ============================================
 * ================ Received ==================
 * ============================================
 */

/**
 * 控制台输出：
 * 2022-11-26 18:11:41,882 source send for key{1} with trade{1000}
 * 2022-11-26 18:11:43,887 source send for key{2} with trade{2000}
 * 2022-11-26 18:11:46,893 source send for key{3} with trade{3000}
 * 2022-11-26 18:11:47,898 source send for key{4} with trade{4000}
 * 2022-11-26 18:11:51,906 source send for key{1} with trade{100}
 * 2022-11-26 18:11:53,907 source send for key{2} with trade{200}
 * 2022-11-26 18:11:56,908 source send for key{3} with trade{300}
 * 2022-11-26 18:11:57,910 source send for key{4} with trade{400}
 * 2022-11-26 18:12:01,916 source send for key{1} with trade{10000}
 * 2022-11-26 18:12:03,921 source send for key{2} with trade{20000}
 * 2022-11-26 18:12:06,926 source send for key{3} with trade{30000}
 * 2022-11-26 18:12:07,928 source send for key{4} with trade{40000}
 * 2022-11-26 18:12:11,937 source send for key{1} with trade{1000000000}
 * 2022-11-26 18:12:12,066 output: 4000
 * 2022-11-26 18:12:12,066 output: 2000
 * 2022-11-26 18:12:12,066 output: 1000
 * 2022-11-26 18:12:12,068 window apply, res=1000
 * 2022-11-26 18:12:12,068 window apply, res=2000
 * 2022-11-26 18:12:12,067 window apply, res=4000
 * 2022-11-26 18:12:12,069 output: 3000
 * 2022-11-26 18:12:12,069 window apply, res=3000
 * 2022-11-26 18:12:13,941 source send for key{2} with trade{2000000000}
 * 2022-11-26 18:12:16,946 source send for key{3} with trade{3000000000}
 * 2022-11-26 18:12:17,952 source send for key{4} with trade{4000000000}
 * 2022-11-26 18:12:21,958 source send for key{1} with trade{10}
 * 2022-11-26 18:12:22,120 output: 400
 * 2022-11-26 18:12:22,120 window apply, res=400
 * 2022-11-26 18:12:22,120 output: 300
 * 2022-11-26 18:12:22,121 window apply, res=300
 * 2022-11-26 18:12:22,120 output: 100
 * 2022-11-26 18:12:22,121 window apply, res=100
 * 2022-11-26 18:12:22,121 output: 200
 * 2022-11-26 18:12:22,121 window apply, res=200
 * 2022-11-26 18:12:23,964 source send for key{2} with trade{20}
 * 2022-11-26 18:12:26,967 source send for key{3} with trade{30}
 * 2022-11-26 18:12:27,971 source send for key{4} with trade{40}
 * 2022-11-26 18:12:31,980 source send for key{1} with trade{100000000}
 * 2022-11-26 18:12:32,133 output: 40000
 * 2022-11-26 18:12:32,133 output: 10000
 * 2022-11-26 18:12:32,133 window apply, res=10000
 * 2022-11-26 18:12:32,133 output: 20000
 * 2022-11-26 18:12:32,133 window apply, res=20000
 * 2022-11-26 18:12:32,133 window apply, res=40000
 * 2022-11-26 18:12:32,133 output: 30000
 * 2022-11-26 18:12:32,134 window apply, res=30000
 * 2022-11-26 18:12:33,985 source send for key{2} with trade{200000000}
 * 2022-11-26 18:12:36,988 source send for key{3} with trade{300000000}
 * 2022-11-26 18:12:37,993 source send for key{4} with trade{400000000}
 * 2022-11-26 18:12:42,002 source send for key{1} with trade{10}
 * 2022-11-26 18:12:42,095 output: 4000000000
 * 2022-11-26 18:12:42,095 output: 1000000000
 * 2022-11-26 18:12:42,096 window apply, res=1000000000
 * 2022-11-26 18:12:42,095 output: 2000000000
 * 2022-11-26 18:12:42,096 window apply, res=2000000000
 * 2022-11-26 18:12:42,095 window apply, res=4000000000
 * 2022-11-26 18:12:42,096 output: 3000000000
 * 2022-11-26 18:12:42,096 window apply, res=3000000000
 * 2022-11-26 18:12:44,005 source send for key{2} with trade{20}
 * 2022-11-26 18:12:47,010 source send for key{3} with trade{30}
 * 2022-11-26 18:12:48,016 source send for key{4} with trade{40}
 * 2022-11-26 18:12:52,021 source send for key{1} with trade{100}
 * 2022-11-26 18:12:52,161 output: 40
 * 2022-11-26 18:12:52,161 output: 20
 * 2022-11-26 18:12:52,161 window apply, res=20
 * 2022-11-26 18:12:52,162 output: 30
 * 2022-11-26 18:12:52,162 window apply, res=30
 * 2022-11-26 18:12:52,161 output: 10
 * 2022-11-26 18:12:52,162 window apply, res=10
 * 2022-11-26 18:12:52,161 window apply, res=40
 * 2022-11-26 18:12:54,025 source send for key{2} with trade{200}
 * 2022-11-26 18:12:57,028 source send for key{3} with trade{300}
 * 2022-11-26 18:12:58,034 source send for key{4} with trade{400}
 * 2022-11-26 18:13:02,041 source send for key{1} with trade{1000000}
 * 2022-11-26 18:13:02,164 output: 400000000
 * 2022-11-26 18:13:02,164 output: 200000000
 * 2022-11-26 18:13:02,164 output: 100000000
 * 2022-11-26 18:13:02,165 window apply, res=200000000
 * 2022-11-26 18:13:02,164 window apply, res=400000000
 * 2022-11-26 18:13:02,165 output: 300000000
 * 2022-11-26 18:13:02,165 window apply, res=100000000
 * 2022-11-26 18:13:02,165 window apply, res=300000000
 * 2022-11-26 18:13:04,045 source send for key{2} with trade{2000000}
 * 2022-11-26 18:13:07,047 source send for key{3} with trade{3000000}
 * 2022-11-26 18:13:08,052 source send for key{4} with trade{4000000}
 * 2022-11-26 18:13:12,062 source send for key{1} with trade{10000000}
 * 2022-11-26 18:13:12,224 output: 40
 * 2022-11-26 18:13:12,225 window apply, res=40
 * 2022-11-26 18:13:12,224 output: 20
 * 2022-11-26 18:13:12,224 output: 10
 * 2022-11-26 18:13:12,225 window apply, res=20
 * 2022-11-26 18:13:12,226 output: 30
 * 2022-11-26 18:13:12,226 window apply, res=10
 * 2022-11-26 18:13:12,226 window apply, res=30
 * 2022-11-26 18:13:14,067 source send for key{2} with trade{20000000}
 * 2022-11-26 18:13:17,072 source send for key{3} with trade{30000000}
 * 2022-11-26 18:13:18,076 source send for key{4} with trade{40000000}
 * 2022-11-26 18:13:22,087 source send for key{1} with trade{1000000}
 * 2022-11-26 18:13:22,282 output: 200
 * 2022-11-26 18:13:22,282 output: 100
 * 2022-11-26 18:13:22,282 output: 400
 * 2022-11-26 18:13:22,283 window apply, res=100
 * 2022-11-26 18:13:22,283 window apply, res=200
 * 2022-11-26 18:13:22,283 window apply, res=400
 * 2022-11-26 18:13:22,283 output: 300
 * 2022-11-26 18:13:22,283 window apply, res=300
 * 2022-11-26 18:13:24,090 source send for key{2} with trade{2000000}
 */
@Slf4j
public class TumblingEventTimeWindowStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(200);

        DataStreamSource<ToWindowEvent> source = env.addSource(new RichSourceFunction<ToWindowEvent>() {
            private boolean run = true;

            @Override
            public void run(SourceContext<ToWindowEvent> sourceContext) throws Exception {
                while (run) {
                    //sleep 1s 然后在接下来的6s，为key=1，2，3，4各发一个数据出去，再sleep 3s，然后循环
                    TimeUnit.SECONDS.sleep(1);
                    long pow = (long) Math.pow(10D, ((int) (Math.random() * 10)));
                    sourceContext.collect(ToWindowEvent.builder().country(1).trade(1*pow).timestamp(System.currentTimeMillis()).build());
                    log.info("[Filter] source send for key{{}} with trade{{}}", 1, pow);
                    TimeUnit.SECONDS.sleep(2);
                    sourceContext.collect(ToWindowEvent.builder().country(2).trade(2*pow).timestamp(System.currentTimeMillis()).build());
                    log.info("[Filter] source send for key{{}} with trade{{}}", 2, 2*pow);
                    TimeUnit.SECONDS.sleep(3);
                    sourceContext.collect(ToWindowEvent.builder().country(3).trade(3*pow).timestamp(System.currentTimeMillis()).build());
                    log.info("[Filter] source send for key{{}} with trade{{}}", 3, 3*pow);
                    TimeUnit.SECONDS.sleep(1);
                    sourceContext.collect(ToWindowEvent.builder().country(4).trade(4*pow).timestamp(System.currentTimeMillis()).build());
                    log.info("[Filter] source send for key{{}} with trade{{}}", 4, 4*pow);
                    TimeUnit.SECONDS.sleep(3);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        });

        WatermarkStrategy<ToWindowEvent> watermarkStrategy = WatermarkStrategy.<ToWindowEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner(((toWindowEvent, timestamp) -> toWindowEvent.getTimestamp()));
        SingleOutputStreamOperator<ToWindowEvent> sourceWithWatermarks = source.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<ToWindowEvent, Integer> keyedStream = sourceWithWatermarks.keyBy(new KeySelector<ToWindowEvent, Integer>() {
            @Override
            public Integer getKey(ToWindowEvent s) throws Exception {
                return s.getCountry();
            }
        });

        WindowedStream<ToWindowEvent, Integer, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.of(10, TimeUnit.SECONDS)));

        SingleOutputStreamOperator<Long> res = windowedStream.apply(new WindowFunction<ToWindowEvent, Long, Integer, TimeWindow>() {
            @Override
            public void apply(Integer key, TimeWindow timeWindow, Iterable<ToWindowEvent> iterable, Collector<Long> collector) throws Exception {
                long sum = 0L;
                for (ToWindowEvent ele : iterable) {
                    sum += ele.getTrade();
                }
                collector.collect(sum);
                log.info("[Filter] window apply, res={}", sum);
            }
        });

        res.addSink(new SinkFunction<Long>() {
            @Override
            public void invoke(Long value, Context context) throws Exception {
                log.info("[Filter] output: {}", value);
            }
        }).name("sink");


        env.execute();
    }



}
