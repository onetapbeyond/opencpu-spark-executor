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
