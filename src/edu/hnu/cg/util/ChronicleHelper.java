package edu.hnu.cg.util;

import java.io.IOException;

import edu.hnu.cg.graph.config.Configure;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

public class ChronicleHelper {

	private IndexedChronicle chronicle;


	private ChronicleHelper() {
		try {
			String basePath = Configure.getConfigure().getStringValue("CSRDataPath");
			System.out.println(basePath);
			if(basePath != null){
				chronicle = new IndexedChronicle(basePath);
			}
			else
				throw new GraphFilePropertyNonFoundException("Please specify the graphfile in the config file...");
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GraphFilePropertyNonFoundException e) {
			e.printStackTrace();
		}
	}

	private static class ChronicleHelperHolder{
		private static ChronicleHelper chronicleHelper = new ChronicleHelper();
	}
	
	public static ChronicleHelper newInstance(){
		return ChronicleHelperHolder.chronicleHelper ;
	}
	

	/**
	 * @param obj : object to be write into the chronicle queue
	 * @param length: how mang bytes should be allocated for the obj
	 * 
	 * Not thread safe, this method should only be allowed to invoke by only one thread.
	 * 	
	 * */
	public void write(Object obj,int length) {
		if (obj != null) {
			ExcerptAppender appender = null;
			try {
				appender = chronicle.createAppender();
				appender.startExcerpt(length);
				appender.writeObject(obj);
				appender.finish();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param index : get the data in the given position
	 * 
	 * this method does not change the data in chronicle queue , so this read function is thread safe
	 * and could be invoke by different threads.
	 * */
	public Object read(long index) {
		ExcerptTailer reader = null;
		Object obj = null;
		
		if (!(index < 0 || index > chronicle.size())) {
			try {
				reader = chronicle.createTailer();
				reader.index(index);
				obj = reader.readObject();
				reader.finish();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return obj;
	}

}


class GraphFilePropertyNonFoundException extends Exception {

	private static final long serialVersionUID = 8543780376849632815L;

	public GraphFilePropertyNonFoundException(String info){
		super(info);
	}
}
