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

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import java.util.*;

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
public class ViewerDataStream extends Receiver<Viewer> {

  private long maxDataOnStreamPerSecond = 1;

  public ViewerDataStream(long maxDataOnStreamPerSecond) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.maxDataOnStreamPerSecond = maxDataOnStreamPerSecond;
  }

  public void onStart() {
    new Thread() {
      @Override public void run() {
        simulateData();
      }
    }.start();
  }

  public void onStop() {
  }

  private void simulateData() {

    try {

      boolean interrupted = false;

      while(!isStopped() && !interrupted) {

        int age = Math.abs(rand.nextInt()) % 100;
        String status = maritalStatus.get(Math.abs((rand.nextInt() %
                                          (maritalStatus.size()))));
        Viewer viewer = new Viewer(age, status);
        store(viewer);

        try {
          Thread.sleep((1000/maxDataOnStreamPerSecond));
        } catch(InterruptedException iex) {
          interrupted = true;
        }
      }

    } catch(Exception ex) {
      System.out.println("ViewerDataStream: ex=" + ex);
    }
  }

  private static List<String> maritalStatus =
    Arrays.asList("MARRIED", "SEPARATED", "DIVORCED", "WIDOWED", "NEVER MARRIED");
  private static Random rand = new Random(System.currentTimeMillis());

}
