name := "restapi-sql"

version := "0.1"

scalaVersion := "2.12.8"


scalacOptions += "-Ypartial-unification"

val akkaVersion = "2.6.0"
val akkaHttpVersion = "10.1.10"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.pauldijou" %% "jwt-core" % "3.0.1",
  "de.heikoseeberger" %% "akka-http-json4s" % "1.22.0",
  "org.json4s" %% "json4s-native" % "3.6.1",
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.slf4j" % "slf4j-simple" % "1.7.25",
  "org.json4s" %% "json4s-jackson" % "3.6.7",
  "org.json4s" %% "json4s-ext" % "3.6.7",

  // for scalikejdbc
  "org.scalikejdbc" %% "scalikejdbc"       % "3.2.1",
  "org.scalikejdbc" %% "scalikejdbc-test"   % "3.2.1"   % "test",
  "org.scalikejdbc" %% "scalikejdbc-config"  % "3.2.1",
  "org.scalikejdbc" %% "scalikejdbc-streams" % "3.2.1",
  "org.scalikejdbc" %% "scalikejdbc-joda-time" % "3.2.1",
  "com.h2database"  %  "h2" % "1.4.199",
  "com.zaxxer" % "HikariCP" % "2.7.4",
  "com.jolbox" % "bonecp" % "0.8.0.RELEASE",
  "com.typesafe.slick" %% "slick" % "3.3.2",

  "ch.qos.logback"  %  "logback-classic"   % "1.2.3",
  "io.monix" %% "monix" % "3.0.0-RC3",
  "org.typelevel" %% "cats-core" % "2.0.0-M4",
  "com.github.tasubo" % "jurl-tools" % "0.6"
)
enablePlugins(JavaAppPackaging)
