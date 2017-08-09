REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;

A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);
Tadj = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Adj','undir_AdjT');

jaccard = FOREACH Tadj GENERATE edu.mit.ll.graphulo.pig.algorithm.Jaccard(dbTable,'undir_AdjDeg','undir_Adj_Jaccard');
DUMP jaccard;
