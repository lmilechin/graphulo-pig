/* This script (roughly) demonstrates the following:
 * 
 * 1. Load a file consisting of comma separated values for input data and graph names (/path/to/file,graphName)
 * 2. Insert all the information into Graphulo as Adjacency graphs
 * 3. Load results from a single, hard-coded graph (SimpleAdjInsertTest)
 * 4. Dump the first ten rows from the graph
 *
 */

REGISTER graphulo-pig.jar

-- Load file into Graphulo
A = LOAD 'insertAdjInput.txt' USING PigStorage(',') AS (inputFile:chararray, graphName:chararray);
B = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.InsertAdjacencyGraph('config/localAccumulo.config',inputFile,graphName);

DUMP A;
DUMP B;

-- Read the graph from Graphulo using Accumulo default LOAD function
raw = LOAD 'accumulo://SimpleAdjInsertTest?instance=graphuloLocal&user=root&password=graphuloLocal&zookeepers=localhost'
      USING org.apache.pig.backend.hadoop.accumulo.AccumuloStorage(
      '*', '') AS
      (vertex:chararray, links:map[]);

raw = LIMIT raw 10;
DUMP raw;
