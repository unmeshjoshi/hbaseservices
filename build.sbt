import Settings._

resolvers in ThisBuild += "Cloudera repository" at "https://repository.cloudera.com/artifactory/cloudera-repos"


lazy val aggregatedProjects: Seq[ProjectReference] = Seq(`sqoop`, `spark`)

lazy val `sqoop` = project
  .enablePlugins(DeployApp, DockerPlugin)
  .settings(defaultSettings: _*)
  .settings(
      libraryDependencies ++= Dependencies.SqoopDependencies
  )

lazy val `spark` = project
  .enablePlugins(DeployApp, DockerPlugin)
  .dependsOn(`sqoop`)
  .settings(defaultSettings: _*)
  .settings(
      //for spark which depends on specific json version
      dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
      dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
      dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7",

      libraryDependencies ++= Dependencies.SparkDependencies,
      libraryDependencies ++= Dependencies.SqoopDependencies
  )

val `hbaseservice` = project
  .in(file("."))
  .aggregate(aggregatedProjects: _*)
