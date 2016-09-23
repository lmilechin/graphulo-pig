/* This script (roughly) demonstrates the following:
 * 
 * 1. Load results from a single, hard-coded graph (SimpleAdjInsertTest)
 * 2. Dump the first ten rows from the graph
 * 3. Flatten the maps into triples
 * 4. Dump the flattened triples
 */

REGISTER graphulo-pig.jar

-- Read the graph from Graphulo using Accumulo default LOAD function
raw = LOAD 'accumulo://SimpleAdjInsertTest?instance=graphuloLocal&user=root&password=XXXXX&zookeepers=localhost'
      USING org.apache.pig.backend.hadoop.accumulo.AccumuloStorage('*', '') AS
      (vertex:chararray, links:map[]);

raw = LIMIT raw 10;
DUMP raw;

F = FOREACH raw GENERATE FLATTEN(edu.mit.ll.graphulo.pig.data.ExtractMaps(vertex, vals)) AS (from:chararray, to:chararray, value:int);
DUMP F;
