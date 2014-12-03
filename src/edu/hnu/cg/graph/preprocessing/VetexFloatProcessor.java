package edu.hnu.cg.graph.preprocessing;

public class VetexFloatProcessor implements
		VertexProcessor<Float> {

	@Override
	public Float receiveVertexValue(int _id, String token) {
		
		return (token == null ? null : Float.parseFloat(token));
		
	}

}
