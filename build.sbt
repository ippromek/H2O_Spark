name := "H2O_Spark"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)



//libraryDependencies += "ai.h2o" % "sparkling-water-core_2.11" % sparkVersion
libraryDependencies += "ai.h2o" % "sparkling-water-core_2.11" % "2.1.12"
libraryDependencies += "ai.h2o" % "sparkling-water-examples_2.11" % "2.1.12"

libraryDependencies += "com.thoughtworks.deeplearning" % "deeplearning_2.11" % "2.0.0"