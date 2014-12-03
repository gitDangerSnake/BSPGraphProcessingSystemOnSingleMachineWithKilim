package edu.hnu.cg.graph.preprocessing;

public interface VertexProcessor<VertexValueType> {
	VertexValueType receiveVertexValue(int _id , String token);
}
