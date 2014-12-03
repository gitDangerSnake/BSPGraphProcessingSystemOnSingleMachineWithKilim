package edu.hnu.cg.graph.preprocessing;

public class EdgeDoubleProcessor implements EdgeProcessor<Double> {

	@Override
	public Double receiveEdge(int from, int to, String token) {
		return (token == null ? null : Double.parseDouble(token));
	}

}
