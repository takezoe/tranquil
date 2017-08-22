name := "tranquil"

organization := "com.github.takezoe"

version := "0.0.3-SNAPSHOT"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.h2database" % "h2" % "1.4.192" % "test"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalacOptions := Seq("-deprecation", "-feature")

//unmanagedClasspath in Compile += baseDirectory.value / "src" / "main" / "resources"

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/takezoe/tranquil</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/takezoe/tranquil</url>
    <connection>scm:git:https://github.com/takezoe/tranquil.git</connection>
  </scm>
  <developers>
    <developer>
      <id>takezoe</id>
      <name>Naoki Takezoe</name>
      <email>takezoe_at_gmail.com</email>
      <timezone>+9</timezone>
    </developer>
  </developers>
)
