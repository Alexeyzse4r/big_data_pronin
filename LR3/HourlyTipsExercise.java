package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final String dataPath = parameters.get("input", ExerciseBase.pathToFareData);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiFare> fareEvents = env.addSource(
            fareSourceOrTest(new TaxiFareSource(dataPath, 60, 600))
        );

        DataStream<Tuple3<Long, Long, Float>> maxTipsHourly = fareEvents
            .keyBy(f -> f.driverId)  
            .timeWindow(Time.hours(1))
            .process(new TipAggregator()) 
            .timeWindowAll(Time.hours(1))
            .maxBy(2);

        printOrTest(maxTipsHourly);
        env.execute("Hourly Tips (modified)");
    }

    public static class TipAggregator extends ProcessWindowFunction<
        TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        
        @Override
        public void process(
            Long driverId,  
            Context ctx,  
            Iterable<TaxiFare> faresWindow,
            Collector<Tuple3<Long, Long, Float>> output) throws Exception {
            
            float total = 0F;
            for (TaxiFare fare : faresWindow) {
                total += fare.tip;
            }
            output.collect(Tuple3.of(
                ctx.window().getEnd(), 
                driverId, 
                total
            ));
        }
    }
}
