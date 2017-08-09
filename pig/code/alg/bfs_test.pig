REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;

A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);

-- Directed Graph BFS
Tadj = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'directed_Adj','directed_AdjT');
Tedge = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'directed_Edge','directed_EdgeT');

bfsA = FOREACH Tadj GENERATE edu.mit.ll.graphulo.pig.algorithm.BFS(dbTable,'adjacency','3,',2);
bfsE = FOREACH Tedge GENERATE edu.mit.ll.graphulo.pig.algorithm.BFS(dbTable,'incidence','3,',2);

DUMP bfsA;
DUMP bfsE;


-- Undirected Graph BFS
Tadj = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Adj','undir_AdjT');
Tedge = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Edge','undir_EdgeT');
Tsingle = FOREACH A GENERATE edu.mit.ll.graphulo.pig.backend.DbTableBinder(configFile,'undir_Single');

bfsA = FOREACH Tadj GENERATE edu.mit.ll.graphulo.pig.algorithm.BFS(dbTable,'adjacency','3,',2,'bfs_undirAdj.config');
bfsE = FOREACH Tedge GENERATE edu.mit.ll.graphulo.pig.algorithm.BFS(dbTable,'incidence','3,',2,'bfs_undirInc.config');
bfsS = FOREACH Tsingle GENERATE edu.mit.ll.graphulo.pig.algorithm.BFS(dbTable,'single','3,',2);

DUMP bfsA;
DUMP bfsE;
DUMP bfsS;
