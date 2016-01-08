package io.onetapbeyond.opencpu.spark.executor

import scala.util.Random
import org.apache.spark.rdd.RDD
import io.onetapbeyond.opencpu.r.executor.OCPUTask
import io.onetapbeyond.opencpu.r.executor.OCPUResult

/**
 * OpenCPU Spark Executor RDD Transformations
 * <p>
 * These transformation operations are automatically available
 * on any RDD of type RDD[OCPUTask] through implicit conversions
 * when you import io.onetapbeyond.opencpu.spark.executor.R._.
 */
class R(rdd:RDD[OCPUTask]) {

	/**
	 * Perform OCPUTask analysis using the default OpenCPU
	 * server @ http://localhost:8004/ocpu.
	 * @return an RDD holding OCPUResult
	 */
	def analyze():RDD[OCPUResult] = {
		rdd.map(oTask => oTask.execute())
	}

	/**
	 * Perform OCPUTask analysis using the dedicated OpenCPU
	 * server indicated by endpoint.
	 * @param endpoint OpenCPU server endpoint
	 * @return an RDD holding OCPUResult
	 */
	def analyze(endpoint:String):RDD[OCPUResult] = {
		rdd.map(oTask => oTask.execute(endpoint))
	}

	/**
	 * Perform OCPUTask analysis using the cluster of OpenCPU
	 * servers indicated by endpoints. OCPUTask are randomly
	 * distributed across the cluster for execution.
	 * @param endpoints OpenCPU server endpoints within cluster
	 * @return an RDD holding OCPUResult
	 */
	def analyze(endpoints:Array[String]):RDD[OCPUResult] = {
		val clusterSize = endpoints.size
		rdd.map(oTask => {
			oTask.execute(endpoints(Random.nextInt(clusterSize)))
		})
	}
}

object R {
  implicit def addR(rdd: RDD[OCPUTask]) = new R(rdd)
}
