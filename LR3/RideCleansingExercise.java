package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansingExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final String rideDataPath = parameters.get("input", ExerciseBase.pathToRideData);

        final int delay = 60;
        final int speed = 600;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rideStream = env.addSource(
            rideSourceOrTest(new TaxiRideSource(rideDataPath, delay, speed))
        );

        DataStream<TaxiRide> nycRides = rideStream.filter(
            ride -> GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat)
        );

        printOrTest(nycRides);

        env.execute("Taxi Ride Cleansing (modified)");
    }
}
