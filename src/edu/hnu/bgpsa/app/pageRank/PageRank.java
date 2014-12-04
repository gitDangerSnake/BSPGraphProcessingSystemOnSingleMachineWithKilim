package edu.hnu.bgpsa.app.pageRank;

import java.io.IOException;

import edu.hnu.bgpsa.graph.framework.Manager;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.BytesToValueConverter.FloatConverter;

public class PageRank {

	public static void main(String[] args) throws IOException {
		BytesToValueConverter<Float> floatConverter = new FloatConverter();
		PageRankHandler handler = new PageRankHandler();
		Graph<Float, EmptyType, Float> graph = new Graph<Float, EmptyType, Float>( null, floatConverter, null, null, handler);
		graph.preprocess();
		Manager<Float, EmptyType, Float> mgr = new Manager<>(floatConverter, null, floatConverter, handler,4);
		mgr.start();

	}
}
