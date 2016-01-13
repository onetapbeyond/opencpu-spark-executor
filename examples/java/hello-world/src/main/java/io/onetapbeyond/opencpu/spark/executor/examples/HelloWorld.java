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
import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;
import java.util.*;

/*
 * HelloWorld
 *
 * The canonical "Hello, World!" example application that demonstrates
 * the basic usage of ROSE to deliver R analytics capabilities within a
 * Spark batch solution.
 */
public class HelloWorld {

  public static void main(String[] args) {

    try {

      JavaSparkContext sc = initSparkContext();

      /*
       * Initialize a basic batch data source for the example by
       * generating a dataRDD<Integer>.
       */
      JavaRDD<Integer> dataRDD =
      	sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));

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
       * Map over dataRDD<Integer> to produce an rTaskRDD<OCPUTask>.
       * Each OCPUTask executes the R stats::rnorm function on
       * the input data provided to generate a random 
       * normal distribution of result values.
       */
      JavaRDD<OCPUTask> rTaskRDD = dataRDD.map(number -> {

        /*
         * Prepare input data for R stats::rnorm call.
         */
        Map data = new HashMap();
        data.put("n", number);
        data.put("mean", 5);
        /*
         * Build OCPUTask for R stats::rnorm call.
         */	
        return OCPU.R()
                   .pkg("stats")
                   .function("rnorm")
                   .input(data)
                   .library();
      });

      /*
       * Execute ROSE R analysis on rTaskRDD[OCPUTask]
       * in order to generate rTaskResultRDD[OCPUResult].
       */
      JavaRDD<OCPUResult> rTaskResultRDD =
      	rTaskRDD.map(rTask -> rTask.execute(endpoint.value()));

      /*
       * As this is an example application we can simply use the
       * foreach() operation on the RDD to force the computation
       * and to output the results.
       */
      rTaskResultRDD.foreach(rTaskResult -> {
      	System.out.println("HelloWorld: stats::rnorm input=" +
      		rTaskResult.input() + " returned=" + rTaskResult.output());
      });

    } catch(Exception ex) {
      System.out.println("HelloWorld: caught ex=" + ex);
    }

  }

  private static JavaSparkContext initSparkContext() {
    SparkConf conf = new SparkConf().setAppName(APP_NAME);
    return new JavaSparkContext(conf);
  }

  private static String OCPU_SERVER = "http://public.opencpu.org/ocpu";
  private static String APP_NAME = "ROSE Hello World Example";

}