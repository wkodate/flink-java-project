package org.apache.flink.quickstart.stream;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;


public class CheckpointedLongRideAlerts {

    public static void main(String[] args) throws Exception {
        String sourcePath = args[0];
        final int servingSpeed = 600;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(100);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        60,
                        Time.of(10, TimeUnit.SECONDS)
                )
        );
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));

        DataStream<TaxiRide> rides = env.addSource(new CheckpointedTaxiRideSource(sourcePath, servingSpeed));
        rides.filter(new TaxiRideCleansing.NewYorkCityFilter())
                .keyBy("rideId")
                .process(new LongRiderAlertsFunction())
                .print();

        env.execute("Fault Recovery In Action - Flink Training");
    }


    // ProcessFunction
    // https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/operators/process_function.html
    // Working with State
    // https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/state/state.html
    public static class LongRiderAlertsFunction extends ProcessFunction<TaxiRide, TaxiRide> {

        // keyed, managed state holds an END event if the ride has ended, otherwise a START event
        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> startDescriptor = new ValueStateDescriptor<>("saved ride", TaxiRide.class);
            rideState = getRuntimeContext().getState(startDescriptor);
        }

        @Override
        public void processElement(TaxiRide taxiRide, Context context, Collector<TaxiRide> collector) throws Exception {
            TimerService timerService = context.timerService();
            if (taxiRide.isStart) {
                if (rideState.value() == null) {
                    // write the state back
                    rideState.update(taxiRide);
                }
            } else {
                // write the state back
                rideState.update(taxiRide);
            }
            // schedule the next timer
            timerService.registerEventTimeTimer(taxiRide.getEventTime() + 120 * 60 * 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
            // get the state for the key that scheduled the timer
            TaxiRide savedRide = rideState.value();
            if (savedRide != null && savedRide.isStart) {
                // emit the state on timeout
                out.collect(savedRide);
            }
            rideState.clear();
        }
    }

}
