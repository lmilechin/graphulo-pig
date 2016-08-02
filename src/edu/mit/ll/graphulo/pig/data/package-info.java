/**
 * Provides the classes necessary to manipulate Graphulo-formatted objects that have been returned to Pig for processing.
 * <p>
 * The algorithm package provides ways to unpack the Accumulo-read data into form more-usable for Pig.
 * This includes the manipulation of the Map data representation and unpacking of the degree from 
 * the edge information in the Single-Table representation.
 * <p>
 * Handles three types of data representation:
 * <ul>
 * <li>Associative: Vertex-Vertex graph matrix, Inverse graph matrix and degree table.
 * <li>Incidence (Edge): Edge graph matrix, Inverse graph matrix and degree table.
 * <li>Single-Table: All data resides in a single table. Most compact representation.
 * </ul>
 * <p>
 * 
 */
package edu.mit.ll.graphulo.pig.data;