name := "adamtwo"

version := "1.0"

scalaVersion := "2.11.7"


resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "spray repo" at "http://repo.spray.io"
)

//libs
libraryDependencies ++= Seq(
  "org.apache.spark"       %   "spark-core_2.11"        % "1.4.1",
  "org.apache.spark"       %   "spark-sql_2.11"         % "1.4.1",
  "org.apache.spark"       %   "spark-mllib_2.11"       % "1.4.1",
  "org.scalanlp" 		       %   "breeze_2.11" 				    % "0.11.2",
  "org.scalanlp" 		       %   "breeze-natives_2.11" 	  % "0.11.2",
  "io.spray"               %%  "spray-can"     		      % "1.3.3",
  "io.spray"               %%  "spray-routing" 		      % "1.3.3",
  "com.typesafe.slick"     %%  "slick"                  % "3.0.2",
  "com.h2database"         %   "h2"                     % "1.4.188",
  "com.typesafe.akka"      %%  "akka-actor"    		      % "2.3.9",
  "org.scalatest"          %   "scalatest_2.11"         % "3.0.0-M7",
  "org.scalacheck"         %   "scalacheck_2.11"        % "1.12.4",
  "org.scala-lang.modules" %%  "scala-pickling" 	 	    % "0.10.1",
  "org.json4s"             %%  "json4s-native"          % "3.2.10"
)