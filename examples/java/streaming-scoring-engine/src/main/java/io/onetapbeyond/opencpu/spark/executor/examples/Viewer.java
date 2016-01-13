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
 * Viewer
 *
 * Represents a TV viewer, capturing both age and marital status. 
 */
public class Viewer implements java.io.Serializable {

  private int age;
  private String status;

  public Viewer(int age, String status) {
    this.age = age;
    this.status = status;
  }

  /*
   * inputs
   * 
   * A convenience method used to build the required input parameter
   * data for a call to the R function tvscore::tv used by the example 
   * application.
   */
  public Map<String,Object> inputs() {

    Map<String,Object> inputs = new HashMap();
    Map data = new HashMap();
    data.put("age", age);
    data.put("marital", status);
    inputs.put("input", Arrays.asList(data));

    return inputs;
  }

}