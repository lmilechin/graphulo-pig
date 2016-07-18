Graphulo-Pig
========

Graphulo-Pig is a Java library that provides access to graph algorithms created in [Graphulo][] from within the [Apache Pig][] analytic environment.

Graphulo is tested on Accumulo 1.7 and Pig 0.0.15.

[Graphulo]: http://graphulo.mit.edu/
[Apache Pig]: https://pig.apache.org/
[Apache Accumulo]: https://accumulo.apache.org/
[GraphBLAS]: http://istc-bigdata.org/GraphBlas/
[TF-IDF]: https://en.wikipedia.org/wiki/Tf%E2%80%93idf

### How do I get started?
Look at the material in the `docs/` folder, especially the Use and Design slide deck.
Read and run the examples-- see below for how.

### Build
[![Shippable Build Status](https://api.shippable.com/projects/54f27f245ab6cc13528fd44d/badge?branchName=master)](https://app.shippable.com/projects/54f27f245ab6cc13528fd44d/builds/latest)
[![Travis Build Status](https://travis-ci.org/Accla/graphulo.svg?branch=master)](https://travis-ci.org/Accla/graphulo)

Prerequisite: Install [Maven](https://maven.apache.org/download.cgi).

Run `mvn package -DskipTests=true` to compile and build graphulo.
This creates three primary Graphulo artifacts inside the `target/` sub-directory:

1. `graphulo-${version}.jar`         Graphulo binaries, enough for client usage.
Include this on the classpath of Java client applications that call Graphulo functions. 
2. `graphulo-${version}-alldeps.jar` Graphulo + all referenced binaries, for easy server installation.
Include this in the `lib/ext/` directory of Accumulo server installations 
in order to provide the Graphulo code and every class referenced by Graphulo,
so that the Accumulo instance has everything it could possibly need to instantiate Graphulo code.
3. `graphulo-${version}-libext.zip`  ZIP of the JARs of all dependencies.
Unzip this in a D4M installation directory to make Graphulo available in D4M.  

The maven script will build everything on Unix-like systems (including Mac),
as long as the *zip* utility is installed.
On Windows systems, `DBinit.m` may not be built (used in D4M installation). 
See the message in the build after running `mvn package`.
