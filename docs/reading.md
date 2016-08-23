Reading QuickStart
========

Graphulo-Pig is a Java library that provides a connector to graph algorithms created in Graphulo from within the Apache Pig analytic environment.

Graphulo stores its graphs in one of three different formats.
* Adjacency
* Edge
* Single-Table 

## Adjacency Graphs

The following uses the built-in Accumulo LOAD function to read in the values from a simple Adjacency graph.

```
REGISTER graphulo-pig.jar

-- Read the Degree Table using Accumulo default
raw = LOAD 'accumulo://SimpleAdj?instance=graphuloLocal&user=root&password=graphuloLocal&zookeepers=localhost'
      USING org.apache.pig.backend.hadoop.accumulo.AccumuloStorage('*', '') AS
      (vertex:chararray,vals:map[]);
DUMP raw;

F = FOREACH raw GENERATE FLATTEN(edu.mit.ll.graphulo.pig.data.ExtractMaps(vertex, vals)) AS (from:chararray, to:chararray, value:int);
DUMP F;
```

This code can be run from the [example file][adjacency].

Depending on the analytic, the formatting of the *raw* Map is not always useful. Therefore, we include an *ExtractMaps* function that expands each Map element to become its own tuple of the form.

```
(chararray, Map)
(vertex1, [vertex2#1,vertex3#1])
```
to the form

```
(charrarray, chararray, int)
(vertex1, vertex2, 1)
(vertex1, vertex3, 1)
```
 
## Edge Graphs

The following uses the built-in Accumulo LOAD function to read in the values from a simple Adjacency graph.

```
REGISTER graphulo-pig.jar

-- Read the Degree Table using Accumulo default
raw = LOAD 'accumulo://SimpleAdj?instance=graphuloLocal&user=root&password=graphuloLocal&zookeepers=localhost'
      USING org.apache.pig.backend.hadoop.accumulo.AccumuloStorage('*', '') AS
      (vertex:chararray,vals:map[]);
DUMP raw;

F = FOREACH raw GENERATE FLATTEN(edu.mit.ll.graphulo.pig.data.ExtractMaps(vertex, vals)) AS (from:chararray, to:chararray, value:int);
DUMP F;
```

Depending on the analytic, the formatting of the *raw* Map is not always useful. Therefore, we include an *ExtractMaps* function that expands each Map element to become its own tuple of the form.

```
(chararray, Map)
(edge1, [:in|vertex2#1,:out|vertex1#1])
(edge2, [:in|vertex3#1,:out|vertex1#1])
```

to the form

```
(charrarray, chararray, int)
(edge1, in|vertex2, 1)
(edge1, out|vertex1, 1)
(edge2, in|vertex3, 1)
(edge2, out|vertex1, 1)
```

We can provide further dissection of the edges as follows:

```
(charrarray, chararray, chararray, int)
(edge1, in, vertex2, 1)
(edge1, out, vertex1, 1)
(edge2, in, vertex3, 1)
(edge2, out, vertex1, 1)
```

## Adjacency and Edge Degree Tables
```
REGISTER graphulo-pig.jar

-- Read the Degree Table using Accumulo default
raw = LOAD 'accumulo://SimpleAdj?instance=graphuloLocal&user=root&password=graphuloLocal&zookeepers=localhost'
      USING org.apache.pig.backend.hadoop.accumulo.AccumuloStorage('*', '') AS
      (vertex:chararray,vals:map[]);
DUMP raw;

F = FOREACH raw GENERATE FLATTEN(edu.mit.ll.graphulo.pig.data.ExtractMaps(vertex, vals)) AS (from:chararray, to:chararray, value:int);
DUMP F;
```

[adjacency]: ../pig/code/insert/graph_adj_test.pig
