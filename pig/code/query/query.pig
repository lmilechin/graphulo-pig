-- Register Graphulo and Graphulo Pig jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /path/to/d4m_api/lib/graphulo-3.0.0.jar;

DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE D4mQuery edu.mit.ll.graphulo.pig.backend.D4mQuery();

-- Load DB Configuration and Bind to Tables
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, adjTable:chararray, transposeTable:chararray);
Tadj = FOREACH A GENERATE DbTableBinder(configFile,'undir_Adj','undir_AdjT');

-- Query for rows with Row IDs 9, 2, and 5
Q = FOREACH Tadj GENERATE FLATTEN(D4mQuery(dbTable,'9,2,5',':'));
dump Q;