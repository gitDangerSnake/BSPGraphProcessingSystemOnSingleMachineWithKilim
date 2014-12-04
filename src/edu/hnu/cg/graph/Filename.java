package edu.hnu.cg.graph;

public class Filename {

	
	/*
	 * 图的输入格式EdgeList时，预处理过程边的shovel中间文件->该文件是紧凑的边二进制文件
	 */
	public static String shovelFilename(String graphfile){
		return graphfile+".shovel";
	}
	
	/*
	 * 图的输入格式EdgeList时，
	 * 预处理过程如果一条边的两个顶点相同则认为该边表示的是顶点的value->该文件是就是这种情况下的顶点的临时文件
	 */
	public static String shovelValueFilename(String graphfile){
		return graphfile+".v.shovel";
	}
	
	
	/*
	 * 图的顶点value的内存映射文件
	 */
	public static String vertexValueFilename(String graphfile){
		return graphfile + "v.mem";
	}
	
	/*
	 * 计算过程中的某个超级步的消息备份文件，该文件的主要作用是用来保证程序的容错性
	 * 如果程序意外终止，可以从CSR文件、value文件与该文件中恢复执行
	 */
	public static String msgTmpFilename(String graphfile,int superstep){
		return graphfile+".msg"+superstep+".bak";
	}
	
}
