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
 * TwitterDataSource
 *
 * To simplify the deployment of this example application this class
 * simulates a Twitter "Tweet" data source.
 */
public class TwitterDataSource {

  public static List<String> build() {

    List<String> tweets =
      Arrays.asList("Amazing performance by Bolt.",
                    "The accident left many people injured.",
                    "Great news!",
                    "Another bad hair day for Trump!",
                    "Big winner in this weeks lottery.",
                    "Yet another losing day at the races.", 
                    "Beautiful photos from space show world in best light.",
                    "The worst winter I ever spent was a summer in San Francisco.");
    return tweets;
  }

}

