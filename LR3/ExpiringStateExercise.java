package com.ververica.flinktraining.exercises.datastream_java.process;

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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The "Expiring State" exercise from the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */

public class ExpiringStateExercise extends ExerciseBase {
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;           
		final int servingSpeedFactor = 600; 	

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
				.keyBy(fare -> fare.rideId);

		SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> processed = rides
				.connect(fares)
				.process(new EnrichmentFunction());

		printOrTest(processed.getSideOutput(unmatchedFares));

		env.execute("ExpiringStateExercise (java)");
	    }

    public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        private ValueState<TaxiRide> rideStateStorage;
        private ValueState<TaxiFare> fareStateStorage;

        @Override
        public void open(Configuration config) {
            rideStateStorage = getRuntimeContext().getState(
                new ValueStateDescriptor<>("stored ride", TaxiRide.class));
            fareStateStorage = getRuntimeContext().getState(
                new ValueStateDescriptor<>("stored fare", TaxiFare.class));
        }

        @Override
        public void processElement1(TaxiRide currentRide, Context context, 
                                   Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiFare existingFare = fareStateStorage.value();
            if (existingFare != null) {
                fareStateStorage.clear();
                context.timerService().deleteEventTimeTimer(existingFare.getEventTime());
                out.collect(new Tuple2<>(currentRide, existingFare));
            } else {
                rideStateStorage.update(currentRide);
                context.timerService().registerEventTimeTimer(currentRide.getEventTime());
            }
        }

        @Override
        public void processElement2(TaxiFare currentFare, Context context, 
                                   Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide existingRide = rideStateStorage.value();
            if (existingRide != null) {
                rideStateStorage.clear();
                context.timerService().deleteEventTimeTimer(existingRide.getEventTime());
                out.collect(new Tuple2<>(existingRide, currentFare));
            } else {
                fareStateStorage.update(currentFare);
                context.timerService().registerEventTimeTimer(currentFare.getEventTime());
            }
        }

        @Override
        public void onTimer(long timerTimestamp, OnTimerContext context, 
                          Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            if (fareStateStorage.value() != null) {
                context.output(unmatchedFaresOutput, fareStateStorage.value());
                fareStateStorage.clear();
            }
            if (rideStateStorage.value() != null) {
                context.output(unmatchedRidesOutput, rideStateStorage.value());
                rideStateStorage.clear();
            }
        }
    }
}
