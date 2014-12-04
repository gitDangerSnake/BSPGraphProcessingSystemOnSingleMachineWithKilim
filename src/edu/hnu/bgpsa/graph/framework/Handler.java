package edu.hnu.bgpsa.graph.framework;


public abstract class Handler<V,E,M> {
	
	public abstract V initValue(int id);
	
	public abstract Message genMessage(int from,int to,V val,E weight,int outDegree);
	
	@Deprecated
	protected boolean isCombinerProvided(){
		return false;
	}
	
	// if user override the HookMethod#isCombinerProvied() then this compute must be implemented.
	@Deprecated
	protected abstract V computeWithCombine(V v,M m);
	@Deprecated
	protected abstract M combine(M v1,M v2);
	
	// if user doesn't override the HookMethod#isCombinerProvied() then this compute must be implemented.
	public abstract V compute(V v ,Message msg,int superstep,boolean isFirstMsg);
	
	protected boolean isEqualsProvided(){
		return false;
	}
	
	public abstract boolean isTwoValueEqual(V o,V n);
	
	
}
