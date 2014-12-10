package edu.hnu.bgpsa.app.component;

import edu.hnu.bgpsa.graph.framework.Message;

class CompomentMessage implements Message{
		
		int to;
		int val;
		
		public CompomentMessage(int to,int val){
			this.to = to;
			this.val =val;
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