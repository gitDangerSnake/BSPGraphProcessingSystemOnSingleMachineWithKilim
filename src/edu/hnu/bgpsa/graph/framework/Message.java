package edu.hnu.bgpsa.graph.framework;

public interface Message {
	
	int getFrom();
	int getTo();
	Object getValue();
	int getSuperstep();

}