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
package io.onetapbeyond.opencpu.spark.executor

import io.onetapbeyond.opencpu.spark.executor.R._
import io.onetapbeyond.opencpu.r.executor._
import org.apache.spark._
import org.scalatest._
import scala.collection.JavaConverters._

class OpenCPUSparkExecutorTestSpec
	extends FlatSpec with Matchers with BeforeAndAfter {

	private val batchDataSize = 10
	private val master = "local[2]"
	private val appName = "opencpu-spark-executor-test"
	private var sc: SparkContext = _

	// Prepare SparkContext (sc) ahead of each unit test.
	before {
		val conf = new SparkConf().setMaster(master)
								  .setAppName(appName)
	    sc = new SparkContext(conf)
	}

	// Release SparkContext (sc) following each unit test.
	after {
		if (sc != null)
		sc.stop()
	}

	"Spark RDD[OCPUTask] analyze transformation" should "execute OCPUTask on a dedicated OCPU server." in {

		// Dedicated OCPU sever endpoint.
		val endpoint = "http://public.opencpu.org/ocpu"

		// Prepare sample Spark batch test data.
		val numRDD = sc.parallelize(1 to batchDataSize)

		// Prepare RDD[OCPUTask], the sample OCPUTasks that will
		// analyze the Spark batch sample data.
		val taskRDD = numRDD.map(num => { 

			var scalaInputs = Map("n" -> num, "mean" -> num)
			val javaInputs = scalaInputs.asJava

			// Build and return sample OCPUTask instance.
			OCPU.R()
				.pkg("stats")
				.function("rnorm")
				.input(javaInputs)
				.library()
		})

		// Generate RDD[OCPUResult] by executing the analyze operation
		// on RDD[OCPUTask] using a dedicated OCPU server.
		val resultRDD = taskRDD.analyze(endpoint)
		resultRDD.cache

		// Process sample RDD[OCPUResult].
		val resultCount = resultRDD.count
		val successCount = resultRDD.filter(result => result.success).count

		// Verify RDD[OCPUResult] data.
		assert(resultCount == batchDataSize)
		assert(resultCount == successCount)
	}

	"Spark RDD[OCPUTask] analyze transformation" should "execute OCPUTask across a cluster of OCPU servers." in {

		// Cluster (simulated) of OCPU sever endpoints.
		val endpoints = Array("http://public.opencpu.org/ocpu",
							  "http://public.opencpu.org/ocpu",
							  "http://public.opencpu.org/ocpu")

		// Prepare sample Spark batch test data.
		val numRDD = sc.parallelize(1 to batchDataSize)

		// Prepare RDD[OCPUTask], the sample OCPUTasks that will
		//  analyze the Spark batch sample data.
		val taskRDD = numRDD.map(num => { 

			var scalaInputs = Map("n" -> num, "mean" -> num)
			val javaInputs = scalaInputs.asJava

			// Build and return sample OCPUTask instance.
			OCPU.R()
				.pkg("stats")
				.function("rnorm")
				.input(javaInputs)
				.library()
		})

		// Execute sample OCPUTask across cluster of OCPU servers.
		// Generate RDD[OCPUResult] by executing the analyze operation
		// on RDD[OCPUTask] using a cluster of OCPU servers.
		val resultRDD = taskRDD.analyze(endpoints)
		resultRDD.cache

		// Process sample RDD[OCPUResult].
		val resultCount = resultRDD.count
		val successCount = resultRDD.filter(result => result.success).count

		// Verify RDD[OCPUResult] data.
		assert(resultCount == batchDataSize)
		assert(resultCount == successCount)
	}

}
