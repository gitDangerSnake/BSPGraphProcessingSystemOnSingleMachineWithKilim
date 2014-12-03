package edu.hnu.cg.graph.preprocessing;

public interface EdgeProcessor<EdgeValueType> {
	EdgeValueType receiveEdge(int from ,int to , String token);
}
