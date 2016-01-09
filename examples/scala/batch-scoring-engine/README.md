###Spark Batch Scoring Engine

An example application demonstrating the use of the ROSE library to
deliver R analytics capabilities within a Spark Batch solution. This
example deploys a scoring engine for calculating predictions using
an [R fitted GAM model](https://cran.r-project.org/web/packages/gam/index.html).

####Source

Check out the example source code provided and you'll see that ROSE
integrates seamlessly within any traditional Spark application. The source
code also provides extensive comments that help guide you through
the integration.

####Build

Run the following [scala-sbt](http://www.scala-sbt.org) command within
the `batch-scoring-engine` directory to build a `fatJar` for the example application
that can then be deployed directly to your Spark cluster.

``
sbt clean assembly
``

The generated `fatJar` can be found in the `target/scala-2.10` directory.

####Launch

The simplest way to launch the example application is to use the
[spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html)
shell script provided as part of the Spark distribution.

The submit command you need should look something like this:

```
spark-submit --class io.onetapbeyond.opencpu.spark.executor.examples.BatchScoringEngine --master local[*] /path/to/fat/jar/batch-scoring-engine-assembly-1.0.jar
```
