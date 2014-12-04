package edu.hnu.bgpsa.app.pageRank;

import edu.hnu.bgpsa.graph.framework.Handler;
import edu.hnu.bgpsa.graph.framework.Message;

public class PageRankHandler extends Handler<Float, EmptyType, Float> {

	@Override
	public Float initValue(int id) {
		return 1.0f;
	}

	@Override
	public Message genMessage(int from, int to, Float val, EmptyType weight,int outDegree) {
		return new PageRankMessage(to, val/outDegree);
	}

	@Override
	protected Float computeWithCombine(Float v, Float m) {
		return null;
	}

	@Override
	protected Float combine(Float v1, Float v2) {
		return null;
	}

	@Override
	public Float compute(Float v, Message msg, int superstep,boolean isFirstMsg) {
		if(isFirstMsg){
			return (float) (0.15f + 0.85*(float)msg.getValue());
		}
		return (float) (v+0.85*(float)msg.getValue());
	}

	@Override
	public boolean isTwoValueEqual(Float o, Float n) {
		return Math.abs(o-n)  < 0.0001;
	}

}
