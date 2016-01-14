###Twitter Sentiment Analysis

An example application demonstrating the use of the ROSE library to
deliver R analytics capabilities within a Spark batch solution. This
example determines the sentiment of `Tweets` using [Naive-Bayes
sentiment classification](https://github.com/mananshah99/sentR).

####Source

Check out the example source code provided and you'll see that ROSE
integrates seamlessly within any traditional Spark application. The source
code also provides extensive comments that help guide you through
the integration.

####Build

Run the following [Gradle](http://gradle.org/) command within
the `sentiment-analysis` directory to build a `fatJar` for the example application
that can then be deployed directly to your Spark cluster.

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
spark-submit --class io.onetapbeyond.opencpu.spark.executor.examples.SentimentAnalysis --master local[*] /path/to/fat/jar/sentiment-analysis-[version]-all.jar
```
