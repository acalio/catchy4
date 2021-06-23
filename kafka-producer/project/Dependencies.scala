import sbt._

object Dependencies
{
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"
  lazy val kafka = "org.apache.spark" %% "kafka" % "2.5.0"
  lazy val youtube = "com.google.apis" % "google-api-services-youtube" % "v3-rev222-1.25.0"
}
