-- Register Graphulo and Graphulo Pig jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /path/to/d4m_api/lib/graphulo-3.0.0.jar;

DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE Jaccard edu.mit.ll.graphulo.pig.algorithm.Jaccard();

-- Load DB Configuration and Bind to Tables
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);
Tadj = FOREACH A GENERATE DbTableBinder(configFile,'undir_Adj','undir_AdjT');

-- Run Jaccard
jaccard = FOREACH Tadj GENERATE Jaccard(dbTable,'undir_AdjDeg','undir_Adj_Jaccard');
DUMP jaccard;
