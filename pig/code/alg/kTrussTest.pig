REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;

A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);

-- kTruss Only for Undirected
Tadj = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Adj','undir_AdjT');
Tedge = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Edge','undir_EdgeT');

kTrussA = FOREACH Tadj GENERATE edu.mit.ll.graphulo.pig.algorithm.KTruss(dbTable,'adjacency',3);
kTrussE = FOREACH Tedge GENERATE edu.mit.ll.graphulo.pig.algorithm.KTruss(dbTable,'incidence',3);

DUMP kTrussA;
DUMP kTrussE;
