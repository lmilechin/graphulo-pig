Reading QuickStart
========

Graphulo-Pig is a Java library that provides a connector to graph algorithms created in Graphulo from within the Apache Pig analytic environment.

Graphulo stores its graphs in one of three different formats.
* Adjacency
* Edge
* Single-Table 

## Adjacency Graphs

The following uses the built-in Accumulo LOAD function to read in the values from a simple Adjacency graph.

> (chararray, Map)

```
REGISTER graphulo-pig.jar

-- Read the Degree Table using Accumulo default
raw = LOAD 'accumulo://SimpleAdj?instance=graphuloLocal&user=root&password=graphuloLocal&zookeepers=localhost'
      USING org.apache.pig.backend.hadoop.accumulo.AccumuloStorage('*', '') AS
      (vertex:chararray,vals:map[]);
DUMP raw;
```
However, the formatting of the *raw* Map is not always useful for some processing needs. Therefore, we include an *ExtractMaps* function that expands each Map element to become its own tuple of the form:

> (charrarray, chararray, int)

```
F = FOREACH raw GENERATE FLATTEN(edu.mit.ll.graphulo.pig.data.ExtractMaps(vertex, vals)) AS (from:chararray, to:chararray, value:int);
DUMP F;
```
 