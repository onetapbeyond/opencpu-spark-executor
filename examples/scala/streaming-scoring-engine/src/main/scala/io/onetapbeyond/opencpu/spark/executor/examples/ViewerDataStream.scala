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

import scala.util.Random
import scala.collection.JavaConverters._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/*
 * ViewerDataStream
 *
 * To simplify the deployment of this example application this class
 * simulates a source of streaming data by implementing a custom
 * Spark {@link org.apache.spark.streaming.receiver.Receiver}.
 *
 * This receiver generates a stream of TV viewer data to be processed
 * by the {@link StreamingScoringEngine}.
 */
class ViewerDataStream(maxDataOnStreamPerSecond: Int)
        extends Receiver[Viewer](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    new Thread("Streaming Scoring Engine (SSE) Receiver") {
      override def run() { simulateData() }
    }.start()
  }

  def onStop() {
  }

  private def simulateData() {

    try {

      var interrupted = false

      while(!isStopped && !interrupted) {

        val viewer = Viewer(ViewerDataStream.simAge(),
                            ViewerDataStream.simStatus())
        store(viewer)

        try {
          Thread.sleep((1000/maxDataOnStreamPerSecond))
        } catch {
          case t:Throwable => interrupted = true
        }
      }

    } catch {
     case t: Throwable => println("SimulateDataStream: ex=" + t)
    }
    
  }

}

object ViewerDataStream {

  private val maritalStatus = Array("MARRIED",
                                    "SEPARATED",
                                    "DIVORCED",
                                    "WIDOWED",
                                    "NEVER MARRIED")

  private def simAge():Int = Random.nextInt(100)
  private def simStatus():String = maritalStatus(Random.nextInt(maritalStatus.size))

}

