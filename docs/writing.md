Writing to Graphulo
========

Graphulo-Pig has three different ways to write to Graphulo, depending on the kind of internal representation required. It currently supports three different formats:
* Adjacency
* Incidence (Edge)
* Single-Table 

### Input File Format
At the current time, Graphulo only accepts files of the following formats:

```
vertexFrom,vertexTo,value
```
(value is an optional field)

CAN IT READ FROM AN EDGE FILES?

### Sample Code
Please see the [sample code][adjacency].

### Accumulo

### Incidence (Edge)
Please see the [sample code][incidence].

### Single Table
Please see the [sample code][single].

 
 [adjacency]: ../pig/code/insert/graph_adj_test.pig
 [incidence]: ../pig/code/insert/graph_inc_test.pig
 [single]: ../pig/code/insert/graph_sng_test.pig