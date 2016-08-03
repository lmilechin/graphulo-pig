Breadth-First Search
========

Once the graph is loaded, 

###Required Parameters

* __AccConfigFile:__ Name of file containing Accumulo configuration information
* __GraphTable:__ Name of Accumulo table containing the graph to be searched.
* __v0:__ Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes. v0 may be a range of nodes like "c,:,e,g,k,:,".
* __k:__ Number of steps
* __ResultTable:__ Name of table to store result. Null means don't store the result.
* __BFSConfigFile:__ Name of file containing additional BFS configuration information.


###Configuration File

* __minDegree:__ Minimum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass 0 for no filtering.
* __maxDegree:__ Maximum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass Integer.MAX_VALUE for no filtering.
* __newVisibility:__ Visibility label for new entries created in Rtable and/or RTtable. Null means use the visibility of the parent keys. Important: this is one option for which null (don't change the visibiltity) is distinguished from the empty string (set the visibility of all Keys seen to the empty visibility).
* __useNewTimestamp:__ If true, new Keys written to Rtable/RTtable receive a new timestamp from {@link System#currentTimeMillis()}. If false, retains the original timestamps of the Keys in Etable.