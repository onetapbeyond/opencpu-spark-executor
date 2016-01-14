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
 * SentimentAnalysis
 * 
 * An example application demonstrating the use of the ROSE library to
 * deliver R analytics capabilities within a Spark Batch solution. This
 * example determines the sentiment of "Tweets" using Naive-Bayes
 * sentiment classification.
 * 
 * To simplify the deployment of this example application it implements an
 * internal "Tweet" data source rather than introducing an external dependency.
 * See {@link TwitterDataSource} for details.
 */
public class SentimentAnalysis {

  public static void main(String[] args) {

    try {

      JavaSparkContext sc = initSparkContext();

      /*
       * Initialize the Twitter "Tweet" data source for the example
       * by generating dataRDD<String>.
       */
      JavaRDD<String> dataRDD = sc.parallelize(TwitterDataSource.build());

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
       * Map over dataRDD<String> to produce an RDD<OCPUTask>.
       * Each OCPUTask executes the R sentR::classify.naivebayes
       * function on each Tweet to generate a sentiment score.
       */
      JavaRDD<OCPUTask> rTaskRDD = dataRDD.map(tweet -> {

        Map data = new HashMap();
        data.put("sentences", tweet);

        return OCPU.R()
                   .user("mananshah99")
                   .pkg("sentR")
                   .function("classify.naivebayes")
                   .input(data)
                   .github();
      });

      /*
       * Execute ROSE R analysis on rTaskRDD<OCPUTask>
       * in order to generate rTaskResultRDD<OCPUResult>.
       */
      JavaRDD<OCPUResult> rTaskResultRDD =
        rTaskRDD.map(rTask -> rTask.execute(endpoint.value()));

      /*
       * As this is an example application we can simply use the
       * foreach() operation on the RDD to force the computation
       * and to output the results.
       */
      rTaskResultRDD.foreach(rTaskResult -> {
        System.out.println("SentimentAnalysis: sentR::classify.naivebayes " +
          rTaskResult.input() + " returned=" + rTaskResult.output() + ", error=" + rTaskResult.error());
      });

    } catch(Exception ex) {
      System.out.println("SentimentAnalysis: caught ex=" + ex);
    }
  }

  private static JavaSparkContext initSparkContext() {
    SparkConf conf = new SparkConf().setAppName(APP_NAME);
    return new JavaSparkContext(conf);
  }

  private static String OCPU_SERVER = "http://public.opencpu.org/ocpu";
  private static String APP_NAME = "ROSE Twitter Sentiment Analysis Example";
 
}