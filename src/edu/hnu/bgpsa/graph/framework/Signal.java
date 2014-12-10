package edu.hnu.bgpsa.graph.framework;

public enum Signal {
	MANAGER_ITERATION_START, MANAGER_ITERATION_OVER, 
	CHECK_STATE, CHECK_STATE_OVER, 
	WORKER_DISPATCH_OVER, WORKER_COMPUTE_OVER, 
	ITERATION_UNACTIVE, ITERATION_ACTIVE, OVER, 
	MONITOR_DISPATCH_OVER, MONITOR_COMPUTE_OVER

}