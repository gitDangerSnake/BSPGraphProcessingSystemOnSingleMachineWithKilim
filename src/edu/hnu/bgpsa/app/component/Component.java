package edu.hnu.bgpsa.app.component;

import java.io.IOException;
import edu.hnu.bgpsa.graph.framework.Handler;
import edu.hnu.bgpsa.graph.framework.Manager;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.BytesToValueConverter.IntConverter;

public class Component {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		BytesToValueConverter<Integer> ic = new IntConverter();
		Handler<Integer,EmptyType,Integer> handler = new ComponentHandler();
		Graph<Integer,EmptyType,Integer> g = new Graph<Integer,EmptyType,Integer>(null, ic,  null, null, handler);
		g.preprocess();
		Manager<Integer,EmptyType,Integer> mgr =
				new Manager(ic, null, ic, handler,100);
		mgr.start();
		
	}

}
