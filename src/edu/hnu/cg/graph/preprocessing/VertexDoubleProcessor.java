package edu.hnu.cg.graph.preprocessing;

public class VertexDoubleProcessor implements VertexProcessor<Double> {

	@Override
	public Double receiveVertexValue(int _id, String token) {
		return (token == null ? null : Double.parseDouble(token));
	}

}
