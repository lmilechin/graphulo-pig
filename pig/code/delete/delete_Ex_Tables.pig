-- Register Graphulo and Graphulo Pig jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /path/to/d4m_api/lib/graphulo-3.0.0.jar;

DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE DeleteTable edu.mit.ll.graphulo.pig.backend.DeleteTable();

-- Load DB Configuration
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);

-- Delete Adjacency Tables and Result Tables
Tadj = FOREACH A GENERATE DbTableBinder(configFile,'directed_Adj','directed_AdjT');
del = FOREACH Tadj GENERATE DeleteTable(dbTable,'directed_AdjDeg','undir_Adj','undir_AdjDeg','undir_AdjT','directed_Adj_BFS','undir_Adj_BFS','undir_Adj_Jaccard','undir_Adj_kTruss');
DUMP del;

-- Delete Incidence Tables and Result Tables
Tedge = FOREACH A GENERATE DbTableBinder(configFile,'directed_Edge','directed_EdgeT');
del = FOREACH Tedge GENERATE DeleteTable(dbTable,'undir_Edge_BFS','directed_EdgeDegT','undir_Edge','undir_EdgeT','undir_EdgeDegT','directed_Edge_BFS','undir_Edge_kTruss','undir_Edge_kTrussT');
DUMP del;

-- Delete Single Table and Result Tables
Tsingle = FOREACH A GENERATE DbTableBinder(configFile,'undir_Single');
del = FOREACH Tsingle GENERATE DeleteTable(dbTable,'undir_Single_BFS');
DUMP del;
