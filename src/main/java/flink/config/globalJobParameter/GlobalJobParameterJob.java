package flink.config.globalJobParameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 运行输出
 * Process中获取到配置：online
 * Sink中获取到配置：online
 * Sink中获取到配置：online
 * Sink中获取到配置：online
 * Process中获取到配置：online
 * Sink中获取到配置：online
 * Source中获取到配置：online
 * Process get value: 1668696242305
 * Sink: 23
 * Process get value: 1668696247313
 * Sink: 23
 * Process get value: 1668696252318
 * Sink: 23
 * Process get value: 1668696257322
 * Sink: 23
 */

public class GlobalJobParameterJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(new GlobalConfig(ParameterTool.fromArgs(args).getInt("profile")));

        DataStreamSource<String> source = env.addSource(new RichSourceFunction<String>() {

            private boolean run = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                GlobalConfig config = (GlobalConfig) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                System.out.println("Source中获取到配置：" + config.getString("env"));
                while (this.run) {
                    sourceContext.collect(System.currentTimeMillis() + "");
                    TimeUnit.SECONDS.sleep(5);
                }
            }

            @Override
            public void cancel() {
                this.run = false;
            }
        }, "tickSource");

        SingleOutputStreamOperator<Long> cookedSource = source.process(new ProcessFunction<String, Long>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                GlobalConfig config = (GlobalConfig) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                System.out.println("Process中获取到配置：" + config.getString("env"));
            }

            @Override
            public void processElement(String s, Context context, Collector<Long> collector) throws Exception {
                System.out.println("Process get value: " + s);
                collector.collect(23L);
            }
        }).setParallelism(2);

        cookedSource.addSink(new RichSinkFunction<Long>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                GlobalConfig config = (GlobalConfig) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                System.out.println("Sink中获取到配置：" + config.getString("env"));
            }

            @Override
            public void invoke(Long value, Context context) throws Exception {
                System.out.println("Sink: " + value);
            }
        }).setParallelism(4);

        env.execute("GlobalJobParameterJob");
    }
}
