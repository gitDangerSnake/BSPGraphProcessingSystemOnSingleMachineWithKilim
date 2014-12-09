package edu.hnu.bgpsa.app.component;

import edu.hnu.bgpsa.graph.framework.Handler;
import edu.hnu.bgpsa.graph.framework.Message;

public class ComponentHandler extends Handler<Integer,EmptyType,Integer>{
	
				@Override
				public Integer initValue(int id) {
					return id;
				}
	
				@Override
				public Message genMessage(int from, int to, Integer val, EmptyType weight,int outDegree) {
					return new CompomentMessage(from,to,val);
				}
	
				@Override
				protected Integer computeWithCombine(Integer v, Integer m) {
					return null;
				}
	
				@Override
				protected Integer combine(Integer v1, Integer v2) {
					return null;
				}
	
				@Override
				public Integer compute(Integer v, Message msg, int superstep,boolean isFistMsg) {
					int msgVal = (int)msg.getValue();
					if(v <= (int)msg.getValue())
						return v;
					else
						return msgVal;
				}
	
				@Override
				public boolean isTwoValueEqual(Integer o, Integer n) {
					return ((int)o == (int)n);
				}
}

