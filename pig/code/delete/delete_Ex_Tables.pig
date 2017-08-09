-- Register Jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;

A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);

Tadj = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'directed_Adj','directed_AdjT');
del = FOREACH Tadj GENERATE edu.mit.ll.graphulo.pig.backend.DeleteTable(dbTable,'directed_AdjDeg','undir_Adj','undir_AdjDeg','undir_AdjT','directed_Adj_BFS','undir_Adj_BFS','undir_Adj_Jaccard','undir_Adj_kTruss');
DUMP del;

Tedge = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'directed_Edge','directed_EdgeT');
del = FOREACH Tedge GENERATE edu.mit.ll.graphulo.pig.backend.DeleteTable(dbTable,'test_Edge_BFS','directed_EdgeDegT','undir_Edge','undir_EdgeT','undir_EdgeDegT','directed_Edge_BFS','undir_Edge_kTruss','undir_Edge_kTrussT');
DUMP del

Tsingle = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Single');
del = FOREACH Tsingle GENERATE edu.mit.ll.graphulo.pig.backend.DeleteTable(dbTable,'undir_Single_BFS');
DUMP del
