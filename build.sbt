import Settings._

resolvers ++= Seq(
    "Cloudera repository" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

val `hbaseservice` = project
  .in(file("."))
  .enablePlugins(DeployApp, DockerPlugin)
  .settings(defaultSettings: _*)
  .settings(
    //for spark which depends on specific json version
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
    dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7",

    libraryDependencies ++= Dependencies.HbaseService
)
