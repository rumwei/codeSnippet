package flink.dataStreamApi.windowedStream;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * ============================================
 * ================ Received ==================
 * ============================================
 */

/**
 * 控制台输出：可以看到在准点50，00，10，20等s时，触发窗口计算
 * 2022-11-27 15:40:46,251 source send for key{1} with trade{10000000}
 * 2022-11-27 15:40:48,254 source send for key{2} with trade{20000000}
 * 2022-11-27 15:40:50,005 output: 20000000
 * 2022-11-27 15:40:50,005 window apply, res=20000000
 * 2022-11-27 15:40:50,005 output: 60000000
 * 2022-11-27 15:40:50,005 window apply, res=60000000
 * 2022-11-27 15:40:51,259 source send for key{3} with trade{30000000}
 * 2022-11-27 15:40:52,265 source send for key{4} with trade{40000000}
 * 2022-11-27 15:40:56,275 source send for key{1} with trade{10}
 * 2022-11-27 15:40:58,280 source send for key{2} with trade{20}
 * 2022-11-27 15:41:00,003 output: 60
 * 2022-11-27 15:41:00,003 window apply, res=60
 * 2022-11-27 15:41:00,006 output: 40000000
 * 2022-11-27 15:41:00,006 output: 30000000
 * 2022-11-27 15:41:00,006 window apply, res=40000000
 * 2022-11-27 15:41:00,006 window apply, res=30000000
 * 2022-11-27 15:41:00,006 output: 20
 * 2022-11-27 15:41:00,007 window apply, res=20
 * 2022-11-27 15:41:01,286 source send for key{3} with trade{30}
 * 2022-11-27 15:41:02,291 source send for key{4} with trade{40}
 * 2022-11-27 15:41:06,296 source send for key{1} with trade{1000000000}
 * 2022-11-27 15:41:08,299 source send for key{2} with trade{2000000000}
 * 2022-11-27 15:41:10,002 output: 30
 * 2022-11-27 15:41:10,002 window apply, res=30
 * 2022-11-27 15:41:10,002 output: 6000000000
 * 2022-11-27 15:41:10,002 window apply, res=6000000000
 * 2022-11-27 15:41:10,002 output: 40
 * 2022-11-27 15:41:10,003 window apply, res=40
 * 2022-11-27 15:41:10,002 output: 2000000000
 * 2022-11-27 15:41:10,003 window apply, res=2000000000
 * 2022-11-27 15:41:11,301 source send for key{3} with trade{3000000000}
 * 2022-11-27 15:41:12,306 source send for key{4} with trade{4000000000}
 * 2022-11-27 15:41:16,316 source send for key{1} with trade{100000000}
 * 2022-11-27 15:41:18,321 source send for key{2} with trade{200000000}
 * 2022-11-27 15:41:20,001 output: 600000000
 * 2022-11-27 15:41:20,001 window apply, res=600000000
 * 2022-11-27 15:41:20,001 output: 3000000000
 * 2022-11-27 15:41:20,001 output: 4000000000
 * 2022-11-27 15:41:20,002 window apply, res=3000000000
 * 2022-11-27 15:41:20,002 window apply, res=4000000000
 * 2022-11-27 15:41:20,002 output: 200000000
 * 2022-11-27 15:41:20,002 window apply, res=200000000
 * 2022-11-27 15:41:21,325 source send for key{3} with trade{300000000}
 * 2022-11-27 15:41:22,329 source send for key{4} with trade{400000000}
 * 2022-11-27 15:41:26,335 source send for key{1} with trade{1}
 * 2022-11-27 15:41:28,339 source send for key{2} with trade{2}
 * 2022-11-27 15:41:30,003 output: 300000000
 * 2022-11-27 15:41:30,004 window apply, res=300000000
 * 2022-11-27 15:41:30,003 output: 400000000
 * 2022-11-27 15:41:30,003 output: 6
 * 2022-11-27 15:41:30,004 window apply, res=6
 * 2022-11-27 15:41:30,004 window apply, res=400000000
 * 2022-11-27 15:41:30,004 output: 2
 * 2022-11-27 15:41:30,005 window apply, res=2
 * 2022-11-27 15:41:31,344 source send for key{3} with trade{3}
 * 2022-11-27 15:41:32,348 source send for key{4} with trade{4}
 * 2022-11-27 15:41:36,357 source send for key{1} with trade{100}
 * 2022-11-27 15:41:38,361 source send for key{2} with trade{200}
 * 2022-11-27 15:41:40,004 output: 600
 * 2022-11-27 15:41:40,004 window apply, res=600
 * 2022-11-27 15:41:40,004 output: 3
 * 2022-11-27 15:41:40,004 output: 4
 * 2022-11-27 15:41:40,004 window apply, res=4
 * 2022-11-27 15:41:40,004 window apply, res=3
 * 2022-11-27 15:41:40,005 output: 200
 * 2022-11-27 15:41:40,005 window apply, res=200
 * 2022-11-27 15:41:41,366 source send for key{3} with trade{300}
 * 2022-11-27 15:41:42,368 source send for key{4} with trade{400}
 * 2022-11-27 15:41:46,373 source send for key{1} with trade{1000000}
 * 2022-11-27 15:41:48,378 source send for key{2} with trade{2000000}
 * 2022-11-27 15:41:50,004 output: 300
 * 2022-11-27 15:41:50,004 window apply, res=300
 * 2022-11-27 15:41:50,004 output: 2000000
 * 2022-11-27 15:41:50,004 window apply, res=2000000
 * 2022-11-27 15:41:50,004 output: 6000000
 * 2022-11-27 15:41:50,004 output: 400
 * 2022-11-27 15:41:50,005 window apply, res=400
 * 2022-11-27 15:41:50,005 window apply, res=6000000
 * 2022-11-27 15:41:51,381 source send for key{3} with trade{3000000}
 * 2022-11-27 15:41:52,387 source send for key{4} with trade{4000000}
 * 2022-11-27 15:41:56,394 source send for key{1} with trade{1000000000}
 * 2022-11-27 15:41:58,397 source send for key{2} with trade{2000000000}
 * 2022-11-27 15:42:00,005 output: 3000000
 * 2022-11-27 15:42:00,005 output: 4000000
 * 2022-11-27 15:42:00,005 output: 6000000000
 */
@Slf4j
public class TumblingProcessTimeWindowStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ToWindowEvent> source = env.addSource(new RichSourceFunction<ToWindowEvent>() {
            private boolean run = true;

            @Override
            public void run(SourceContext<ToWindowEvent> sourceContext) throws Exception {
                while (run) {
                    //sleep 1s 然后在接下来的6s，为key=1，2，3，4各发一个数据出去，再sleep 3s，然后循环
                    TimeUnit.SECONDS.sleep(1);
                    long pow = (long) Math.pow(10D, ((int) (Math.random() * 10)));
                    sourceContext.collect(ToWindowEvent.builder().country(1).trade(1*pow).timestamp(System.currentTimeMillis()).build());
                    sourceContext.collect(ToWindowEvent.builder().country(1).trade(5*pow).timestamp(System.currentTimeMillis()).build());
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

        KeyedStream<ToWindowEvent, Integer> keyedStream = source.keyBy(new KeySelector<ToWindowEvent, Integer>() {
            @Override
            public Integer getKey(ToWindowEvent s) throws Exception {
                return s.getCountry();
            }
        });

        WindowedStream<ToWindowEvent, Integer, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.of(10, TimeUnit.SECONDS)));

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
