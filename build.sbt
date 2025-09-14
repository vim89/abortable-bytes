// ===== GLOBAL BUILD SETTINGS =====
ThisBuild / organization := "vim"

ThisBuild / version           := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion      := "3.7.0"
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := "4.9.7" // good for Metals

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wconf:msg=unused:info",
  // kyo
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error",
  "-language:strictEquality"
)

// Ensure your app runs in a separate JVM (so sbt memory != app memory)
fork := true

ThisBuild / Test / parallelExecution := false
ThisBuild / Test / testOptions += Tests.Argument("-oDF")
ThisBuild / Test / fork := true
ThisBuild / Test / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

lazy val root = (project in file("."))
  .settings(
    name := "abortable-bytes",
    libraryDependencies ++= Seq(
      "org.typelevel"         %% "cats-effect"          % "3.5.4",
      "co.fs2"                %% "fs2-io"               % "3.10.2",
      "io.getkyo"             %% "kyo-prelude"          % "0.19.0", // 3.3.x
      "io.getkyo"             %% "kyo-core"             % "0.19.0", // 3.3.x
      "io.getkyo"             %% "kyo-cats"             % "0.19.0", // 3.3.x
      "com.google.cloud"       % "google-cloud-storage" % "2.36.0"
    ),
    // Make absolutely sure no 3.7 stdlib sneaks in:
    // dependencyOverrides += "org.scala-lang" %% "scala3-library" % "3.6.2",
    excludeDependencies += "org.scala-lang" % "scala-reflect" // keep stray Scala-2 off the classpath
  )

// ===== SBT ALIASES =====
addCommandAlias("compileAll", ";compile; test:compile")
