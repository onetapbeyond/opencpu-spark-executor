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

import scala.collection.JavaConverters._

/*
 * Viewer
 *
 * Represents a TV viewer, capturing both age and marital status. 
 */
case class Viewer(age:Int, status:String) {

  /*
   * inputs
   * 
   * A convenience method used to build the required input parameter
   * data for a call to the R function tvscore::tv used by the example 
   * application.
   *
   * As the ROSE library depends on the Java opencpu-r-executor library
   * these inputs must be converted to a Java compatible Map before being
   * returned on this call.
   */
  def inputs():Map[String,Object] = {
    val pdata = Map("age" -> age,
                    "marital" -> status)
    val plist = List(pdata.asJava)
    Map("input" -> plist.asJava) 
  }

}