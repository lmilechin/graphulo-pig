-- Register Graphulo and Graphulo Pig jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /path/to/d4m_api/lib/graphulo-3.0.0.jar;

DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE KTruss edu.mit.ll.graphulo.pig.algorithm.KTruss();

-- Load DB Configuration and Bind to Tables
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);
Tadj = FOREACH A GENERATE DbTableBinder(configFile,'undir_Adj','undir_AdjT');
Tedge = FOREACH A GENERATE DbTableBinder(configFile,'undir_Edge','undir_EdgeT');

-- kTruss Only for Undirected
kTrussA = FOREACH Tadj GENERATE KTruss(dbTable,'adjacency',3);
kTrussE = FOREACH Tedge GENERATE KTruss(dbTable,'incidence',3);

DUMP kTrussA;
DUMP kTrussE;
