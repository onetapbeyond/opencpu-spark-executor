###Spark Streaming Scoring Engine

An example application demonstrating the use of the ROSE library to
deliver R analytics capabilities within a Spark Streaming solution.
This example deploys a scoring engine for calculating realtime
predictions using an
[R fitted GAM model](https://cran.r-project.org/web/packages/gam/index.html).

####Source

Check out the example source code provided and you'll see that ROSE
integrates seamlessly within any traditional Spark application. The source
code also provides extensive comments that help guide you through
the integration.

####Build

Run the following [Gradle](http://gradle.org/) command within
the `streaming-scoring-engine` directory to build a `fatJar` for the
example application that can then be deployed directly to your Spark cluster.

``
gradlew clean shadowJar
``

The generated `fatJar` can be found in the `build/libs` directory.

####Launch

The simplest way to launch the example application is to use the
[spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html)
shell script provided as part of the Spark distribution.

The submit command you need should look something like this:

```
spark-submit --class io.onetapbeyond.opencpu.spark.executor.examples.StreamingScoringEngine --master local[*] /path/to/fat/jar/streaming-scoring-engine-[version]-all.jar
```
