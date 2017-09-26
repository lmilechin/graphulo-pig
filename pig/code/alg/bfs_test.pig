-- Register Graphulo and Graphulo Pig jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /path/to/d4m_api/lib/graphulo-3.0.0.jar;

DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE BFS edu.mit.ll.graphulo.pig.algorithm.BFS();

-- Load DB Configuration
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);

-- Directed Graph BFS
Tadj = FOREACH A GENERATE DbTableBinder(configFile,'directed_Adj','directed_AdjT');
Tedge = FOREACH A GENERATE DbTableBinder(configFile,'directed_Edge','directed_EdgeT');

bfsA = FOREACH Tadj GENERATE BFS(dbTable,'adjacency','3,',2);
bfsE = FOREACH Tedge GENERATE BFS(dbTable,'incidence','3,',2);

DUMP bfsA;
DUMP bfsE;

-- Undirected Graph BFS
Tadj = FOREACH A GENERATE DbTableBinder(configFile,'undir_Adj','undir_AdjT');
Tedge = FOREACH A GENERATE DbTableBinder(configFile,'undir_Edge','undir_EdgeT');
Tsingle = FOREACH A GENERATE DbTableBinder(configFile,'undir_Single');

bfsA = FOREACH Tadj GENERATE BFS(dbTable,'adjacency','3,',2);
bfsE = FOREACH Tedge GENERATE BFS(dbTable,'incidence','3,',2,'bfs_undirInc.config');
bfsS = FOREACH Tsingle GENERATE BFS(dbTable,'single','3,',2);

DUMP bfsA;
DUMP bfsE;
DUMP bfsS;
