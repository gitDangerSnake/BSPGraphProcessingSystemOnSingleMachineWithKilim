package edu.hnu.cg.util;

import java.io.IOException;
import java.util.Arrays;

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
				System.out.println("get exception at " + index);
				e.printStackTrace();
			}
		}
		return obj;
	}
	
	public static void main(String[] args) throws InterruptedException {
		ChronicleHelper h = ChronicleHelper.newInstance();
		for(int i=0;i<4000000;i++){
			Object obj = h.read(i);
			if(obj instanceof byte[]){
				byte[] arr = (byte[])obj;
				System.out.print(i +" : [ ");
				for(int k=0;k<arr.length/4;k+=4){
					int id =  ((arr[i] & 0xff) << 24) + ((arr[i + 1] & 0xff) << 16) 
							+ ((arr[i + 2] & 0xff) << 8) + (arr[i + 3] & 0xff); // 计算出边的目的顶点
					System.out.print(","+id);
				}
				
				System.out.println("]");
			}else if(obj!=null){
				System.out.println(i + " " + obj);
			}else if(obj==null){
				System.out.println(i+ " null");
			}
		}
	}

}


class GraphFilePropertyNonFoundException extends Exception {

	private static final long serialVersionUID = 8543780376849632815L;

	public GraphFilePropertyNonFoundException(String info){
		super(info);
	}
}
