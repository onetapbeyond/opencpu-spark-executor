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
import scala.collection.JavaConverters._

/*
 * BatchScoringEngine
 * 
 * An example application demonstrating the use of the ROSE library to
 * deliver R analytics capabilities within a Spark Batch solution. This
 * example deploys a scoring engine for calculating predictions using
 * an R fitted GAM model.
 * 
 * To understand in detail the R analytics used by this example see the
 * documentation in the following article posted on the OpenCPU blog:
 * 
 * https://www.opencpu.org/posts/scoring-engine/
 * 
 * To simplify the deployment of this example application it implements an
 * internal batch data source rather than introducing an external dependency.
 * See {@link ViewerDataSource} for details.
 */
object BatchScoringEngine {

  def main(args:Array[String]):Unit = {

    try {

      val sc = initSparkContext()

      /*
       * Initialize the batch data source for the example by
       * generating an RDD[Viewer].
       */
      val dataRDD =
        sc.parallelize(ViewerDataSource.build(BATCH_DATA_SIZE))
	
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
       * Map over dataRDD[Viewer] to produce an RDD[OCPUTask].
       * Each OCPUTask executes the R tvscore::tv function on the
       * input data provided to generate a realtime prediction.
       */
      val rTaskRDD = dataRDD.map(viewer => {

        OCPU.R()
            .user("opencpu")
            .pkg("tvscore")
            .function("tv")
            .input(viewer.inputs().asJava)
            .github()
      })

      /*
       * Apply the ROSE analyze transformation to rTaskRDD[OCPUTask]
       * in order to generate RDD[OCPUResult].
       */
      val rResultRDD = rTaskRDD.analyze(endpoint.value)

      /*
       * As this is an example application we can use the
       * foreach() operation on the RDD to force the computation
       * and to output the results.
       */
      rResultRDD.foreach { result => {
        println("BatchScoringEngine: " + "tvscore::tv input=" +
                  result.input + " returned=" + result.output)
      }}

    } catch {
      case t:Throwable => println("BatchScoringEngine: caught ex=" + t)
    }

  }

  def initSparkContext():SparkContext = {
    val conf = new SparkConf().setAppName(APP_NAME)
    new SparkContext(conf)
  }

  private val OCPU_SERVER = "http://public.opencpu.org/ocpu"
  private val APP_NAME = "ROSE Batch Scoring Engine Example"
  private val BATCH_DATA_SIZE = 10

}