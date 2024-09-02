import sbtassembly.AssemblyPlugin.autoImport._

scalaVersion := "2.12.18"

name := "brazil-ecommerce"
organization := "ch.epfl.scala"
version := "1.0"

// Thêm các dependency cần thiết cho Spark và ScalaTest
libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.5.1",
  "org.scalatest" %% "scalatest" % "3.2.16" % Test,
  "mysql" % "mysql-connector-java" % "8.0.30",
  "org.postgresql" % "postgresql" % "42.5.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.5", // Hoặc phiên bản phù hợp
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.5" // Hoặc phiên bản phù hợp
)

// Plugin sbt-assembly
enablePlugins(AssemblyPlugin)

// Chỉ định thư mục lưu file JAR
assembly / target := baseDirectory.value / "../jobs"  // Đường dẫn đến thư mục jobs

// Tùy chỉnh sbt-assembly
assemblyJarName in assembly := "emcommerce-etl.jar"

// Quản lý các lớp trùng lặp
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
