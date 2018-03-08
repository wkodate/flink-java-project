package org.apache.flink.quickstart.stream;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


/**
 * Skeleton for a Flink Streaming Job.
 * <p>
 * <p>For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 * <p>
 * <p>You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * target/flink-java-project-0.1.jar
 * From the CLI you can then run
 * ./bin/flink run -c org.apache.flink.quickstart.stream.StreamingJob target/flink-java-project-0.1.jar
 * <p>
 * <p>For more information on the CLI see:
 * <p>
 * <p>http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        String sourcePath = args[0];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        int maxDelay = 1;
        int servingSpeed = 1;
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(sourcePath, maxDelay, servingSpeed));
        rides.addSink(new FlinkKafkaProducer011<TaxiRide>(
                        "localhost:9092",
                        "cleansedRides",
                        new TaxiRideSchema())
        );


        /**
         rides.filter()
         .flatMap()
         .join()
         */

        rides.print();
        env.execute("Flink Streaming Java API Skeleton");

        // Example output
        // 1> 936656,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.9867,40.751236,-74.00318,40.72036,2
        // 1> 928769,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.99078,40.76088,-73.954185,40.778847,1
        // 1> 899206,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.97391,40.747868,-73.9489,40.77665,5
    }
}
