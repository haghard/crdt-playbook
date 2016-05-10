import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import bintray.Keys._
import NativePackagerKeys._

organization := "haghard.github"

name := "crdt-playbook"

version := "0.0.1-snapshot"

scalaVersion := "2.11.8"

parallelExecution := false
parallelExecution in Test := false
logBuffered in Test := false

initialCommands in console in Test := "import org.specs2._"

shellPrompt := { state => System.getProperty("user.name") + "> " }

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-Yno-adapted-args",
  "-target:jvm-1.8"
)

useJGit
enablePlugins(GitVersioning)
enablePlugins(JavaAppPackaging)

val akkaVersion = "2.4.4"

val localMvnRepo = "/Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository"

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  "Local Maven Repository" at "file:///" + localMvnRepo,
  "maven central"          at "http://repo.maven.apache.org/maven2",
  "jboss repo"             at "http://repository.jboss.org/nexus/content/groups/public-jboss/"
)

libraryDependencies ++= Seq(
  "com.rbmhtechnology" %% "eventuate-core"          % "0.7",
  "com.rbmhtechnology" %% "eventuate-crdt"          % "0.7",
  "org.scalaz.stream"  %% "scalaz-stream"           % "0.8",
  "io.dmitryivanov"    %% "scala-crdt"              % "1.0",
  "com.typesafe.akka"  %% "akka-typed-experimental" % akkaVersion,
  "com.typesafe.akka"  %% "akka-slf4j"              % akkaVersion,
  "ch.qos.logback"     % "logback-classic"          % "1.1.6"
)

libraryDependencies ++= Seq(
  "ch.qos.logback"     % "logback-classic" % "1.1.6",
  "com.typesafe.akka"  %% "akka-multi-node-testkit" % akkaVersion % Test,
  "org.scalatest"      %% "scalatest" % "2.2.6" % Test
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-deprecation",
  "-unchecked",
  "-Ywarn-dead-code",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:existentials")

javacOptions ++= Seq(
  "-source", "1.8",
  "-target", "1.8",
  "-Xlint:unchecked",
  "-Xlint:deprecation")

seq(bintraySettings:_*)

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/"))

bintrayOrganization in bintray := Some("haghard")

repository in bintray := "snapshots" //"releases"

publishMavenStyle := true
//publishTo := Some(Resolver.file("file",  new File(localMvnRepo)))

cancelable in Global := true

//create/update for Compile and Test configurations, add the following settings to your build
inConfig(Compile)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))
inConfig(Test)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))


//bintray:: tab
//bintray::publish