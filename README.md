Graphulo-Pig
========

Graphulo-Pig is a Java library that provides a connector to graph algorithms created in [Graphulo][] from within the [Apache Pig][] analytic environment.

The Graphulo-Pig connector is tested on Accumulo 1.7 and Pig 0.0.15.

[Graphulo]: http://graphulo.mit.edu/
[Apache Pig]: https://pig.apache.org/
[Apache Accumulo]: https://accumulo.apache.org/

### Prerequisites
Graphulo-Pig requires access to existing installations of:

1. [Apache Accumulo][] with [Graphulo]
2. [Apache Pig]

Instructions on setting up each environment can be found on their appropriate websites.

### Build

Prerequisites:

1. Install [Maven](https://maven.apache.org/download.cgi).

Run `mvn package -DskipTests=true` to compile and build graphulo-pig.
This creates the primary Graphulo-Pig artifact inside the `target/` sub-directory:

1. `graphulo-pig-${version}.jar`         Graphulo-Pig binaries, enough for client usage.
Include this on the classpath of Java client applications that call Graphulo functions. 

The maven script should build everything on Unix-like systems (including Mac).
