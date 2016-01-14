/*
 * Copyright 2016 David Russell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.onetapbeyond.opencpu.spark.executor.examples;

import io.onetapbeyond.opencpu.r.executor.*;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function;
import java.util.*;

/*
 * StreamingScoringEngine
 *
 * An example application demonstrating the use of the ROSE library to
 * deliver R analytics capabilities within a Spark Streaming solution.
 * This example deploys a scoring engine for calculating realtime
 * predictions using an R fitted GAM model.
 * 
 * To understand in detail the R analytics used by this example see the
 * documentation in the following article posted on the OpenCPU blog:
 * 
 * https://www.opencpu.org/posts/scoring-engine/
 * 
 * To simplify the deployment of this example application it implements
 * an intenral streaming data source rather than introducing an external
 * dependency. The internal streaming data source is implemented as a custom
 * Spark {@link org.apache.spark.streaming.receiver.Receiver} that simulates
 * a stream of TV viewer data, the data used by the example scoring engine.
 * See {@link ViewerDataStream} for details.
 */
public class StreamingScoringEngine {

  public static void main(String[] args) {

    try {

      JavaSparkContext sc = initSparkContext();
      JavaStreamingContext ssc = initSparkStreamingContext(sc);

      /*
       * Initialize the viewer data input stream for the example.
       */
      JavaDStream<Viewer> dataStream =
        ssc.receiverStream(new ViewerDataStream(DATA_PER_SECOND));

      /*
       * Because this example depends on an OpenCPU cloud server that
       * lives external to the Spark cluster it is good practice to
       * register the endpoint as a Spark broadcast variable so it
       * can be safely referenced later by the call to OCPUTask.execute.
       * Passing an endpoint on the OCPUTask.execute operation is not
       * required if you deploy your application to a Spark cluster
       * that has an OpenCPU server running on each Spark worker node.
       */
      Broadcast<String> endpoint = sc.broadcast(OCPU_SERVER);

      /*
       * Transform dataStream<Viewer> to produce rTaskStream<OCPUTask>.
       * Each OCPUTask executes the R tvscore::tv function on the
       * input data provided to generate a realtime prediction.
       */
      JavaDStream<OCPUTask> rTaskStream = dataStream.map(viewer -> {

        /*
         * Build OCPUTask for R tvscore::tv call.
         */	
        return OCPU.R()
                   .user("opencpu")
                   .pkg("tvscore")
                   .function("tv")
                   .input(viewer.inputs())
                   .github();
      });

      /*
       * Execute ROSE R analysis on rTaskRDD<OCPUTask>
       * in order to generate rTaskResultRDD<OCPUResult>.
       */
      JavaDStream<OCPUResult> rTaskResultStream =
      rTaskStream.map(rTask -> rTask.execute(endpoint.value()));

      /*
       * As this is an example application we can simply use the
       * foreachRDD() operation on the stream to force the
       * computation and to output the results.
       */
      rTaskResultStream.foreachRDD(rdd -> {

        rdd.foreach(rTaskResult -> { 
          System.out.println("StreamingScoringEngine: " +
              "tvscore::tv input=" + rTaskResult.input() + 
              " returned=" + rTaskResult.output());
        });
        return null;
      });

      /*
       * Start the Spark streaming example application.
       */
      ssc.start();

      /*
       * Limit the runtime duration of the example application
       * to APP_TIMEOUT so we do not overload the public
       * OpenCPU server used by default in this application.
       */
      ssc.awaitTerminationOrTimeout(APP_TIMEOUT);

    } catch(Exception ex) {
      System.out.println("StreamingScoringEngine: caught ex=" + ex);
    }

  }

  private static JavaSparkContext initSparkContext() {
    SparkConf conf = new SparkConf().setAppName(APP_NAME);
    return new JavaSparkContext(conf);
  }

  private static JavaStreamingContext initSparkStreamingContext(JavaSparkContext sc) {
    return new JavaStreamingContext(sc, new Duration(BATCH_DURATION));
  }

  private static String OCPU_SERVER = "http://public.opencpu.org/ocpu";
  private static String APP_NAME = "ROSE Streaming Predictive Scoring Engine Example";
  private static long   BATCH_DURATION = 1000;
  private static long   DATA_PER_SECOND = 3;
  private static long   APP_TIMEOUT = 10000;

}