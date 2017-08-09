/* This script (roughly) demonstrates the following:
 * 
 * 1. Load results from a single, hard-coded graph (SimpleAdjInsertTest)
 * 2. Dump the first ten rows from the graph
 * 3. Flatten the maps into triples
 * 4. Dump the flattened triples
 */

REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;

A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, adjTable:chararray, transposeTable:chararray);
Tadj = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'test_Adj','test_AdjT');

TadjDeg = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_AdjDeg');

Q = FOREACH Tadj GENERATE FLATTEN(edu.mit.ll.graphulo.pig.backend.D4mQuery(dbTable,':',':'));
dump Q;
