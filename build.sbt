name := "spark-root"

organization := "org.diana-hep"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

version := "0.1-pre1"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

spIgnoreProvided := true
sparkVersion := "2.0.0"
sparkComponents := Seq("sql")

resolvers += Resolver.mavenLocal
libraryDependencies += "org.diana-hep" % "root4j" % "1.0-pre2"
