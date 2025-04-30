package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        ParameterTool arguments = ParameterTool.fromArgs(args);
        String ridesPath = arguments.get("rides", pathToRideData);
        String faresPath = arguments.get("fares", pathToFareData);

        int maxDelay = 60;
        int speedFactor = 1800;

        Configuration configuration = new Configuration();
        configuration.setString("state.backend", "filesystem");
        configuration.setString("state.savepoints.dir", "file:///tmp/savepoints");
        configuration.setString("state.checkpoints.dir", "file:///tmp/checkpoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rideStream = env
            .addSource(rideSourceOrTest(new TaxiRideSource(ridesPath, maxDelay, speedFactor)))
            .filter(ride -> ride.isStart)
            .keyBy("rideId");

        DataStream<TaxiFare> fareStream = env
            .addSource(fareSourceOrTest(new TaxiFareSource(faresPath, maxDelay, speedFactor)))
            .keyBy("rideId");

        DataStream<Tuple2<TaxiRide, TaxiFare>> joinedStream = rideStream
            .connect(fareStream)
            .flatMap(new RideFareMatcher())
            .uid("ride-fare-matcher");

        printOrTest(joinedStream);

        env.execute("Rides and Fares Join (modified)");
    }

    public static class RideFareMatcher extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        private transient ValueState<TaxiRide> pendingRide;
        private transient ValueState<TaxiFare> pendingFare;

        @Override
        public void open(Configuration parameters) throws Exception {
            pendingRide = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-ride", TaxiRide.class)
            );
            pendingFare = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-fare", TaxiFare.class)
            );
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            TaxiFare fare = pendingFare.value();
            if (fare != null) {
                pendingFare.clear();
                collector.collect(Tuple2.of(ride, fare));
            } else {
                pendingRide.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            TaxiRide ride = pendingRide.value();
            if (ride != null) {
                pendingRide.clear();
                collector.collect(Tuple2.of(ride, fare));
            } else {
                pendingFare.update(fare);
            }
        }
    }
}
