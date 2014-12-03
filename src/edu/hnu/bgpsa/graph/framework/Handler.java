package edu.hnu.bgpsa.graph.framework;


public abstract class Handler<V,E,M> {
	
	abstract Message genMessage(int from,int to,V val,E weight);
	
	@Deprecated
	protected boolean isCombinerProvided(){
		return false;
	}
	
	// if user override the HookMethod#isCombinerProvied() then this compute must be implemented.
	@Deprecated
	abstract V compute(V v,M m);
	@Deprecated
	protected abstract M combine(M v1,M v2);
	
	// if user doesn't override the HookMethod#isCombinerProvied() then this compute must be implemented.
	abstract V compute(V v ,Message msg,int superstep);
	
	protected boolean isEqualsProvided(){
		return false;
	}
	
	abstract boolean isTwoValueEqual(V o,V n);
	
	
}
