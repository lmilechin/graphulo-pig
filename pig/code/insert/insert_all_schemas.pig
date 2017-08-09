
-- Register Jars
REGISTER ../../../target/graphulo-pig-0.0.1-SNAPSHOT.jar;
REGISTER /home/gridsan/tools/d4m_api/lib/graphulo-3.0.0.jar;


DEFINE DbTableBinder edu.mit.ll.graphulo.pig.backend.DbTableBinder();
DEFINE PutTriple edu.mit.ll.graphulo.pig.backend.PutTriple();
-- Ingest Directed Graphs
A = LOAD 'dbSetup.txt' USING PigStorage() AS (configFile:chararray, mainTable:chararray, transposeTable:chararray);
TadjDir = FOREACH A GENERATE DbTableBinder(configFile,'directed_Adj','directed_AdjT');
TedgeDir = FOREACH A GENERATE DbTableBinder(configFile,'directed_Edge','directed_EdgeT');

fnames = LOAD 'insertAdjInput.txt' USING PigStorage(',') AS (fname:chararray, graphName:chararray);

ingestA = FOREACH TadjDir GENERATE PutTriple(dbTable, 'adjacency', '../../data/sample.graph', 'DeleteTables', true, 'Directed', true);
ingestI = FOREACH TedgeDir GENERATE PutTriple(dbTable, 'incidence', '../../data/sample.graph', 'Directed', true, 'DeleteTables', true);

DUMP ingestA;
DUMP ingestI;

-- Ingest Undirected Graphs
TadjUnDir = FOREACH A GENERATE DbTableBinder(configFile,'undir_Adj','undir_AdjT');
TedgeUnDir = FOREACH A GENERATE DbTableBinder(configFile,'undir_Edge','undir_EdgeT');
Tsingle = FOREACH A GENERATE DbTableBinder(configFile,'undir_Single');

ingestA = FOREACH TadjUnDir GENERATE PutTriple(dbTable, 'adjacency', '../../data/undirected.graph', 'DeleteTables', true);
ingestI = FOREACH TedgeUnDir GENERATE PutTriple(dbTable, 'incidence', '../../data/undirIncidence.graph', 'DeleteTables', true, 'InputSchema', 'incidence');
ingestS = FOREACH Tsingle GENERATE PutTriple(dbTable, 'single', '../../data/sample.graph', 'DeleteTables', true);

DUMP ingestA;
DUMP ingestI;
DUMP ingestS;
