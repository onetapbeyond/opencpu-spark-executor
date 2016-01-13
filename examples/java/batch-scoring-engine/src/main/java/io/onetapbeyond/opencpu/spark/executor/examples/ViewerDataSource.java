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

import java.util.*;

/*
 * ViewerDataSource
 *
 * To simplify the deployment of this example application this class
 * simulates a batch data source.
 */
public class ViewerDataSource {

  public static List<Viewer> build(int batchDataSize) {

    List<Viewer> viewers = new ArrayList();

    for(int i=0; i < batchDataSize; i++) {
      int age = Math.abs(rand.nextInt()) % 100;
      String status = maritalStatus.get(Math.abs((rand.nextInt() %
                                        (maritalStatus.size()))));
      Viewer v = new Viewer(age, status);
      viewers.add(v);
    }

    return viewers;
  }

  private static List<String> maritalStatus =
    Arrays.asList("MARRIED", "SEPARATED", "DIVORCED", "WIDOWED", "NEVER MARRIED");
  private static Random rand = new Random(System.currentTimeMillis());
}

