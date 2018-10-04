name := "spark-root"

organization := "org.diana-hep"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

version := "0.1.17"
//isSnapshot := true

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

// spark
spIgnoreProvided := true

sparkVersion := "2.3.0"

sparkComponents := Seq("sql")

resolvers += Resolver.mavenLocal

libraryDependencies += "org.diana-hep" % "root4j" % "0.1.6"

// publishing to Maven
publishMavenStyle := true

// repos to push to
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

//credentials += Credentials(Path.userHome / ".sbt" / ".sonaytype_credentials")

// not to publish your test artifacts
publishArtifact in Test := false

// include the log4j
libraryDependencies += "org.apache.logging.log4j" % "log4j" % "2.8"

// scalatest
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"

pomExtra := (
  <url>https://github.com/diana-hep/spark-root</url>
  <scm>
    <url>git@github.com:diana-hep/spark-root.git</url>
    <connection>scm:git:git@github.com:diana-hep/spark-root.git</connection>
  </scm>
  <developers>
    <developer>
        <name>Jim Pivarski</name>
        <email>jpivarski@gmail.com</email>
        <organization>DIANA-HEP</organization>
        <organizationUrl>http://diana-hep.org</organizationUrl>
    </developer>
    <developer>
      <id>vkhristenko</id>
      <name>Viktor Khristenko</name>
      <url>https://github.com/diana-hep/spark-root</url>
    </developer>
  </developers>)
