name := "citibike"

version := "1.0"

scalaVersion := "2.11.8"

lazy val spark = "2.0.0"

val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-streaming" % spark,
  "org.apache.spark" %% "spark-yarn" % spark,
  "org.apache.spark" %% "spark-mllib" % spark
)

libraryDependencies ++= sparkDeps.map(_ % "provided")

libraryDependencies ++= Seq(
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "mysql" % "mysql-connector-java" % "5.1.37",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run))
