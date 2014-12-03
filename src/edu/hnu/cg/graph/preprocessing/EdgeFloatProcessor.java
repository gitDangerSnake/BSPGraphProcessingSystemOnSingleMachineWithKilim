package edu.hnu.cg.graph.preprocessing;

public class EdgeFloatProcessor implements EdgeProcessor<Float> {

	@Override
	public Float receiveEdge(int from, int to, String token) {
		return (token == null ? null : Float.parseFloat(token));
	}

}
