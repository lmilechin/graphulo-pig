
-- Register Graphulo and Graphulo Pig jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;

DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE PutTriple edu.mit.ll.graphulo.pig.backend.PutTriple();
DEFINE D4mQuery edu.mit.ll.graphulo.pig.backend.D4mQuery();
DEFINE BFS edu.mit.ll.graphulo.pig.algorithm.BFS();
DEFINE Jaccard edu.mit.ll.graphulo.pig.algorithm.Jaccard();
DEFINE KTruss edu.mit.ll.graphulo.pig.algorithm.KTruss();
DEFINE DeleteTable edu.mit.ll.graphulo.pig.backend.DeleteTable();

-- Bind to Table
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);
Tadj = FOREACH A GENERATE DbTableBinder(configFile,mainTable,transposeTable);

-- Ingest Data
fnames = LOAD 'ingestFiles.txt' USING PigStorage(',') AS (fname:chararray);
ingest = FOREACH fnames GENERATE PutTriple(Tadj.dbTable, 'adjacency', fname);

DUMP ingest;

-- Query Data
Q = FOREACH Tadj GENERATE FLATTEN(D4mQuery(dbTable,'1,3,',':'));
dump Q;

-- Breadth First Search
bfs = FOREACH Tadj GENERATE BFS(dbTable,'adjacency','3,',2);
DUMP bfs;

-- Jaccard
jaccard = FOREACH Tadj GENERATE Jaccard(dbTable,'TadjDeg','Tadj_Jaccard');
DUMP jaccard;

-- k-Truss Subgraph
kTruss = FOREACH Tadj GENERATE KTruss(dbTable,'adjacency',3);
DUMP kTruss;

-- Delete Tables
del = FOREACH Tadj GENERATE DeleteTable(dbTable,'TadjDeg','undir_AdjDeg','Tadj_BFS','Tadj_Jaccard','Tadj_kTruss');
DUMP del;

