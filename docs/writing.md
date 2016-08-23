Writing to Graphulo
========

Graphulo-Pig has three different ways to write to Graphulo, depending on the kind of internal representation required. It currently supports three different formats:
* Adjacency
* Incidence (Edge)
* Single-Table 

Additional information on the differences between the representations can be found in [Graphulo's documentation][TODO].

### Adjacency Graph File Format
At the current time, Graphulo only accepts files of the following formats:

```
vertexFrom,vertexTo,value
```

Value is an optional field. If it's not included, Graphulo will default to a value of 1.

### Edge Graph File Format

TODO: Verify it can read from edge files. Or put it on the future features list.

### Sample Code
There is sample code for reading a file containing an adjacency graph:

* [Adjacency graphs][adjacency]
* [Incidence (edge) graphs][incidence]
* [Single table graphs][single]

There will be sample code for reading a file containing an edge graph
* [Incidence (edge) graphs][TODO]

 
 [adjacency]: ../pig/code/insert/graph_adj_test.pig
 [incidence]: ../pig/code/insert/graph_inc_test.pig
 [single]: ../pig/code/insert/graph_sng_test.pig
 [TODO]: .