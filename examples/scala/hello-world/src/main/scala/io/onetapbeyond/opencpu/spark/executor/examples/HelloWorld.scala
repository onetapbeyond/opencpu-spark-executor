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
 * HelloWorld
 *
 * The canonical "Hello, World!" example application that demonstrates
 * the basic usage of ROSE to deliver R analytics capabilities within a
 * Spark batch solution.
 */
object HelloWorld {

  def main(args:Array[String]):Unit = {

    try {

      val sc = initSparkContext()

      /*
       * Initialize a basic batch data source for the example by
       * generating an RDD[Int].
       */
      val dataRDD = sc.parallelize(1 to BATCH_DATA_SIZE)

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
       * Map over dataRDD[Int] to produce an RDD[OCPUTask].
       * Each OCPUTask executes the R stats::rnorm function on
       * the input data provided to generate a random 
       * normal distribution of result values.
       */
      val rTaskRDD = dataRDD.map(num => {

        /*
         * Input data for R stats::rnorm function call.
         */
        var rInputs = Map("n" -> 4, "mean" -> num)

        OCPU.R()
            .pkg("stats")
            .function("rnorm")
            .input(rInputs.asJava)
            .library()
      })

      /*
       * Apply the ROSE analyze transformation to rTaskRDD[OCPUTask]
       * in order to generate RDD[OCPUResult].
       */
      val rTaskResultRDD = rTaskRDD.analyze(endpoint.value)

      /*
       * As this is an example application we can simply use the
       * foreach() operation on the RDD to force the computation
       * and to output the results.
       */
      rTaskResultRDD.foreach { result => {
        println("HelloWorld: stats::rnorm input=" +
          result.input + " returned=" + result.output)
      }}

    } catch {
      case t:Throwable => println("HelloWorld: caught ex=" + t)
    }

  }

  def initSparkContext():SparkContext = {
    val conf = new SparkConf().setAppName(APP_NAME)
    new SparkContext(conf)
  }

  private val OCPU_SERVER = "http://public.opencpu.org/ocpu"
  private val APP_NAME = "ROSE Hello World Example"
  private val BATCH_DATA_SIZE = 10

}
