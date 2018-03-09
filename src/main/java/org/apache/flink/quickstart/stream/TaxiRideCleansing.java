package org.apache.flink.quickstart.stream;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


public class TaxiRideCleansing {

    public static void main(String[] args) throws Exception {
        String sourcePath = args[0];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final int maxDelay = 60;
        final int servingSpeed = 600;

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(sourcePath, maxDelay, servingSpeed));
        rides.filter(new NewYorkCityFilter())
                .print();

        env.execute("Taxi Ride Cleansing - Flink Training");

    }


    public static class NewYorkCityFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }


}
