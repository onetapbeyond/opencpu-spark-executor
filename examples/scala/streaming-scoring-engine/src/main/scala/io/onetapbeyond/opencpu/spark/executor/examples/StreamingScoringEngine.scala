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
package io.onetapbeyond.opencpu.spark.executor.examples

import io.onetapbeyond.opencpu.spark.executor.R._
import io.onetapbeyond.opencpu.r.executor._
import org.apache.spark._
import org.apache.spark.streaming._
import scala.collection.JavaConverters._

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
object StreamingScoringEngine {

  def main(args:Array[String]):Unit = {

    try {

      val sc = initSparkContext()
      val ssc = initSparkStreamingContext(sc)

      /*
       * Initialize the viewer data input stream for the example.
       */
      val dataStream = ssc.receiverStream(new ViewerDataStream(
                                      STREAM_DATA_PER_SECOND))
	
      /*
       * Because this example depends on an OpenCPU cloud server that
       * lives external to the Spark cluster it is good practice to
       * register the endpoint as a Spark broadcast variable so it
       * can be safely referenced later by the call to RDD.analyze.
       * Passing an endpoint on the RDD.analyze operation is not
       * required if you deploy your application to a Spark cluster
       * that has an OpenCPU server running on each Spark worker node.
       */			
      val endpoint = sc.broadcast(OCPU_SERVER)

      /*
       * Transform dataStream[Viewer] to produce rTaskStream[OCPUTask].
       * Each OCPUTask executes the R tvscore::tv function on the
       * input data provided to generate a realtime prediction.
       */
      val rTaskStream = dataStream.transform(rdd => {

        rdd.map(viewer => {

          OCPU.R()
              .user("opencpu")
              .pkg("tvscore")
              .function("tv")
              .input(viewer.inputs().asJava)
              .github()
        })

      })

      /*
       * Apply the ROSE analyze transformation to generate a stream
       * of RDD[OCPUResult].
       */
      val rResultStream =
        rTaskStream.transform(rdd => rdd.analyze(endpoint.value))

      /*
       * As this is an example application we can use the
       * foreach() operation on the rResultStream to force the
       * computation and to output the results.
       */
      rResultStream.foreachRDD { rdd => {
        rdd.foreach { result => {
          println("StreamingScoringEngine: " + "tvscore::tv input=" +
                          result.input + " returned=" + result.output)
        }}
      }}

      /*
       * Start the Spark streaming example application.
       */
      ssc.start() 

      /*
       * Limit the runtime duration of the example application
       * to APP_TIMEOUT so we do not overload the public
       * OpenCPU server used by default in this application.
       */
      ssc.awaitTerminationOrTimeout(APP_TIMEOUT)

    } catch {
      case t:Throwable => println("StreamingScoringEngine: caught ex=" + t)
    }
  }

  def initSparkContext():SparkContext = {
    val conf = new SparkConf().setAppName(APP_NAME)
    new SparkContext(conf)
  }

  def initSparkStreamingContext(sc:SparkContext):StreamingContext = {
    new StreamingContext(sc, Seconds(STREAM_BATCH_DURATION))
  }

  private val OCPU_SERVER = "http://public.opencpu.org/ocpu"
  private val APP_NAME = "ROSE Streaming Predictive Scoring Engine Example"
  private val STREAM_BATCH_DURATION = 1
  private val STREAM_DATA_PER_SECOND = 3
  private val APP_TIMEOUT = 10000

}
