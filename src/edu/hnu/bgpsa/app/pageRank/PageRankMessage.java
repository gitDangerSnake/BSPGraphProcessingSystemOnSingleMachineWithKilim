package edu.hnu.bgpsa.app.pageRank;

import edu.hnu.bgpsa.graph.framework.Message;

public class PageRankMessage implements Message{
	int to;
	float val;
	
	public PageRankMessage(int to,float val){
		this.to = to;
		this.val = val;
	}

	@Override
	public int getFrom() {
		return -1;
	}

	@Override
	public int getTo() {
		return to;
	}

	@Override
	public Object getValue() {
		return val;
	}

	@Override
	public int getSuperstep() {
		return -1;
	}

}
