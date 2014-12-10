package edu.hnu.cg.graph;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.hnu.bgpsa.app.component.ComponentHandler;
import edu.hnu.bgpsa.app.pageRank.EmptyType;
import edu.hnu.bgpsa.app.pageRank.PageRankHandler;
import edu.hnu.bgpsa.graph.Exception.WrongNumberOfWorkersException;
import edu.hnu.bgpsa.graph.framework.Handler;
import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.BytesToValueConverter.FloatConverter;
import edu.hnu.cg.graph.BytesToValueConverter.IntConverter;
import edu.hnu.cg.graph.config.Configure;
import edu.hnu.cg.graph.preprocessing.EdgeProcessor;
import edu.hnu.cg.graph.preprocessing.VertexProcessor;
import edu.hnu.cg.util.BufferedDataInputStream;
import edu.hnu.cg.util.ChronicleHelper;
import edu.hnu.cg.util.Helper;

public class Graph<VertexValueType, EdgeValueType, MsgValueType> {

	private static Logger logger;
	private static Configure config = Configure.getConfigure();

	public static String baseFilePath;
	public static String format;
	public static String adjDataPath;
	public static int nvertices;
	public static int MAXID;

	public static int nworkers;
	public static int cachelineSize;
	public static boolean cacheLineEnabled;
	public static int vertexIdBytesLength;
	public static int lengthBytesLength;
	public static int degreeBytesLength;
	public static int valueOffsetWidth;

	public static byte[] cachelineTemplate;
	public static int[] lengthsOfWorkerMsgsPool;

	private static final String vertexToEdgeSeparate = ":";
	private static final String idToValueSeparate = ",";
	private static final String edgeToEdgeSeparate = "->";

	static {

		// get configurations
		baseFilePath = config.getStringValue("baseFilePath");
		format = config.getStringValue("format");
		adjDataPath = config.getStringValue("CSRDataPath");
		nvertices = config.getInt("nvertices");
		MAXID = config.getInt("maxId");
		nworkers = config.getInt("nworkers");
		if ((nworkers & (nworkers - 1)) != 0)
			try {
				throw new WrongNumberOfWorkersException(
						"The number of workers should be the 2^N...");
			} catch (WrongNumberOfWorkersException e) {
				e.printStackTrace();
			}

		cachelineSize = config.getInt("cachelineSize");
		String str = config.getStringValue("cacheLineEnabled");
		if (str.equals("true"))
			cacheLineEnabled = true;
		vertexIdBytesLength = config.getInt("vertexIdBytesLength");
		degreeBytesLength = config.getInt("degreeBytesLength");
		valueOffsetWidth = config.getInt("valueOffsetWidth");
		lengthBytesLength = config.getInt("lengthBytesLength");

		cachelineTemplate = new byte[config.getInt("cachelineSize")];

		logger = Logger.getLogger("Graph PreProcessing");
	}

	public static int getMaxID() {
		return MAXID;
	}

	/**
	 * enum type: 图的格式
	 * */
	public enum graphFormat {
		EDGELIST, ADJACENCY
	};

	/*
	 * 图的文件名
	 */
	private String graphFilename;

	/*
	 * 图的边的条数
	 */
	private long numEdges;
	/*
	 * 图的顶点的个数
	 */
	public long numVertices;

	/*
	 * vertexProcessor： 处理顶点内value的处理器，字符转换为bytes
	 */
	private VertexProcessor<VertexValueType> vertexProcessor;

	/*
	 * edgeProcessor： 处理边上权重的处理器，字符转换为bytes
	 */
	private EdgeProcessor<EdgeValueType> edgeProcessor;

	/*
	 * 数据转换器：字节数组转换为数据EdgeValueType
	 */
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;

	/*
	 * 数据转换器：字节数组转换为数据VertexValueType
	 */
	private BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter;

	private Handler<VertexValueType, EdgeValueType, MsgValueType> handler;

	/*
	 * 默认的VertexValueType字节缓存数组
	 */
	private byte[] vertexValueTemplate;

	/*
	 * 默认的EdgeValueType字节缓存数组
	 */
	private byte[] edgeValueTemplate;

	private DataOutputStream shovelWriter;
	private DataOutputStream shovelValueWriter;

	private DataOutputStream graphDataValueStream;

	public Graph(
			VertexProcessor<VertexValueType> vertexProcessor,
			EdgeProcessor<EdgeValueType> edgeProcessor,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter,
			byte[] vertexValueTemplate, byte[] edgeValueTemplate,
			DataOutputStream shovelWriter, DataOutputStream shovelValueWriter,
			DataOutputStream graphDataValueStream,
			Handler<VertexValueType, EdgeValueType, MsgValueType> handler) {

		this.graphFilename = baseFilePath;

		this.vertexProcessor = vertexProcessor;
		this.edgeProcessor = edgeProcessor;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		this.verterxValueTypeBytesToValueConverter = verterxValueTypeBytesToValueConverter;

		this.vertexValueTemplate = vertexValueTemplate;
		this.edgeValueTemplate = edgeValueTemplate;
		this.shovelWriter = shovelWriter;
		this.shovelValueWriter = shovelValueWriter;
		this.graphDataValueStream = graphDataValueStream;

		this.handler = handler;
	}

	public Graph(
			VertexProcessor<VertexValueType> vertexProcessor,
			EdgeProcessor<EdgeValueType> edgeProcessor,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter,
			Handler<VertexValueType, EdgeValueType, MsgValueType> handler)
			throws FileNotFoundException {
		this.graphFilename = baseFilePath;

		this.numVertices = nvertices;
		this.vertexProcessor = vertexProcessor;
		this.edgeProcessor = edgeProcessor;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		this.verterxValueTypeBytesToValueConverter = verterxValueTypeBytesToValueConverter;

		this.shovelWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.shovelFilename(graphFilename))));
		this.shovelValueWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(
						Filename.shovelValueFilename(graphFilename))));

		this.graphDataValueStream = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(
						Filename.vertexValueFilename(graphFilename))));

		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTemplate = new byte[edgeValueTypeBytesToValueConverter
					.sizeOf()];
		} else {
			edgeValueTemplate = new byte[0];
		}

		if (verterxValueTypeBytesToValueConverter != null) {
			vertexValueTemplate = new byte[verterxValueTypeBytesToValueConverter
					.sizeOf()];
		} else {
			vertexValueTemplate = new byte[0];
		}

		this.handler = handler;

	}

	public Graph(
			BytesToValueConverter<EdgeValueType> _edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> _verterxValueTypeBytesToValueConverter,
			VertexProcessor<VertexValueType> _vertexProcessor,
			EdgeProcessor<EdgeValueType> _edgeProcessor,
			Handler<VertexValueType, EdgeValueType, MsgValueType> handler)
			throws FileNotFoundException {

		graphFilename = baseFilePath;

		vertexProcessor = _vertexProcessor;
		edgeProcessor = _edgeProcessor;
		edgeValueTypeBytesToValueConverter = _edgeValueTypeBytesToValueConverter;
		verterxValueTypeBytesToValueConverter = _verterxValueTypeBytesToValueConverter;

		this.shovelWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.shovelFilename(graphFilename))));
		this.shovelValueWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(
						Filename.shovelValueFilename(graphFilename))));

		this.graphDataValueStream = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(
						Filename.vertexValueFilename(graphFilename))));

		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTemplate = new byte[edgeValueTypeBytesToValueConverter
					.sizeOf()];
		} else {
			edgeValueTemplate = new byte[0];
		}

		if (verterxValueTypeBytesToValueConverter != null) {
			vertexValueTemplate = new byte[verterxValueTypeBytesToValueConverter
					.sizeOf()];
		} else {
			vertexValueTemplate = new byte[0];
		}

		this.handler = handler;

	}

	public String getGraphFilename() {
		return graphFilename;
	}

	/**
	 * 从Graph plain文件中读取数据并
	 * */
	public void preprocess() throws IOException {
		File chronicleDataFile = new File(adjDataPath + ".data");
		File chronicleIndexFile = new File(adjDataPath + ".index");
		File maxId = new File(adjDataPath+".maxid");

		if (!chronicleDataFile.exists() || !chronicleIndexFile.exists() || !maxId.exists()) {
			if(chronicleDataFile.exists()) chronicleDataFile.delete();
			if(chronicleIndexFile.exists()) chronicleIndexFile.delete();
			if(maxId.exists()) maxId.delete();
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(graphFilename))));
			String ln = null;
			long lnNum = 0;
			if (format.toLowerCase().equals("edgelist")) {
				Pattern p = Pattern.compile("\\s");
				while ((ln = bReader.readLine()) != null) {
					numEdges++;
					if (numEdges % 1000000 == 0) {
						logger.info("Reading line : " + numEdges);
					}
					// String[] tokenStrings = ln.split("\\s");

					String[] tokenStrings = p.split(ln);

					if (tokenStrings.length == 2) {

						addEdge(Integer.parseInt(tokenStrings[0]),
								Integer.parseInt(tokenStrings[1]), null);

					} else if (tokenStrings.length == 3) {

						addEdge(Integer.parseInt(tokenStrings[0]),
								Integer.parseInt(tokenStrings[1]),
								tokenStrings[2]);
					}
				}

			} else if (format.toLowerCase().equals("adjacency")) {
				while ((ln = bReader.readLine()) != null) {
					// id,value : id,value->id,value->id,value
					lnNum++;
					if (lnNum % 1000000 == 0) {
						logger.info("Reading line : " + lnNum);
					}
					extractLine(ln);
				}

			}

			bReader.close();
			edgelist_process();
		} else {
		BufferedReader br= new BufferedReader(new FileReader(maxId));
		String mi = br.readLine();
		try{
			int m = Integer.valueOf(mi);
			MAXID = m;
			br.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
			initValue();
		}

	}

	public void addEdge(int from, int to, String token) throws IOException {
		
		addToShovel( from, to, (edgeProcessor != null ? edgeProcessor.receiveEdge(from, to, token) : null));
	}

	private void addToShovel(int from, int to, EdgeValueType value)
			throws IOException {
		shovelWriter.writeLong(Helper.pack(from, to));
		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTypeBytesToValueConverter.setValue(edgeValueTemplate,
					value);
			shovelWriter.write(edgeValueTemplate);
		}
		shovelWriter.flush();
	}

	public void addVertexValue(int from, VertexValueType value)
			throws IOException {
		shovelValueWriter.writeLong(Helper.pack(0, from));
		verterxValueTypeBytesToValueConverter.setValue(vertexValueTemplate, value);
		shovelValueWriter.write(vertexValueTemplate);

	}

	public void edgelist_process() throws IOException {

		/******************************************************************************************************************
		 * 计算出边上权重字节大小与顶点value的字节大小
		 ******************************************************************************************************************/
		int sizeOfEdgeValue = (edgeValueTypeBytesToValueConverter != null ? edgeValueTypeBytesToValueConverter
				.sizeOf() : 0); // 边上的值的字节大小
		int sizeOfValue = (verterxValueTypeBytesToValueConverter != null ? verterxValueTypeBytesToValueConverter
				.sizeOf() : 4);// 顶点值的size大小

		/******************************************************************************************************************
		 * 获取运行环境的缓存行的大小，为后面的多线程保存value提供并发性保证
		 ******************************************************************************************************************/

		/******************************************************************************************************************
		 * 处理顶点value的shovel文件
		 ******************************************************************************************************************/
		File shovelValueFile = new File(
				Filename.shovelValueFilename(graphFilename));
		shovelValueFile.deleteOnExit();

		long len = shovelValueFile.length();
		long[] vertices = new long[(int) (len / (8 + sizeOfValue))];
		byte[] vertexValues = new byte[vertices.length * sizeOfValue];

		BufferedDataInputStream vin = new BufferedDataInputStream(
				new FileInputStream(shovelValueFile));
		for (int k = 0; k < vertices.length; k++) {
			vertices[k] = vin.readLong();
			vin.readFully(vertexValueTemplate);
			System.arraycopy(vertexValueTemplate, 0, vertexValues, k
					* sizeOfValue, sizeOfValue);
		}
		vin.close();
		// shovelValueFile.delete();
		Helper.quickSort(vertices, vertexValues, sizeOfValue, 0,
				vertices.length - 1);

		/******************************************************************************************************************
		 * 处理边的shovel文件
		 ******************************************************************************************************************/

		File shovelFile = new File(Filename.shovelFilename(graphFilename));
		shovelFile.deleteOnExit();
		long[] edges = new long[(int) shovelFile.length()
				/ (8 + sizeOfEdgeValue)];
		byte[] edgeValues = new byte[edges.length * sizeOfEdgeValue];

		// 处理边
		BufferedDataInputStream in = new BufferedDataInputStream(
				new FileInputStream(shovelFile));
		for (int k = 0; k < edges.length; k++) {
			long l = in.readLong();
			edges[k] = l;
			in.readFully(edgeValueTemplate);
			System.arraycopy(edgeValueTemplate, 0, edgeValues, sizeOfEdgeValue
					* k, sizeOfEdgeValue);
		}

		// numEdges += edges.length;

		in.close();
		// shovelFile.delete();
		Helper.quickSort(edges, edgeValues, sizeOfEdgeValue, 0,
				edges.length - 1);

		/******************************************************************************************************************
		 * 处理边的shovel文件
		 ******************************************************************************************************************/
		int curvid = -1;
		int currentSequence = 0;
		int isstart = 0;

		boolean flag = false;
		byte[] entry = null;
		ChronicleHelper ch = ChronicleHelper.newInstance();
		// 从边构建邻接表
		for (int s = 0; s < edges.length; s++) {
			
			int from = Helper.getFirst(edges[s]); 
			
			if(from != curvid ){
				if(curvid == -1) {
					curvid = from;
					continue;
				}else{
					int outdegree = s-isstart;
					entry = new byte[outdegree*(4+sizeOfEdgeValue)];
					int curstart = 0;
					while(isstart < s){
						
						int to = Helper.getSecond(edges[isstart]);
						if(to>MAXID) MAXID = to;
						System.arraycopy(Helper.intToByteArray(to), 0, entry, curstart, 4);
						curstart += 4;
						System.arraycopy(edgeValues, isstart * sizeOfEdgeValue, entry, curstart, sizeOfEdgeValue);
						curstart += sizeOfEdgeValue;
						isstart++;
						
					}
					while(currentSequence < curvid){
						ch.write((byte)-1, 100);
						currentSequence++;
					}
					
					ch.write(entry, 100+entry.length);
					currentSequence++;
					curvid = from;
					if(curvid > MAXID) MAXID = curvid;
					
				}
			}else if(s == edges.length-1){
				int outdegree = s -isstart + 1;
				
				entry = new byte[outdegree*(4+sizeOfEdgeValue)];
				int curstart = 0;
				
				while(isstart < s+1){
					int to = Helper.getSecond(edges[isstart]);
					if(to>MAXID) MAXID = to;
					System.arraycopy(Helper.intToByteArray(to), 0, entry, curstart, 4);
					curstart += 4;
					System.arraycopy(edgeValues, isstart * sizeOfEdgeValue, entry, curstart, sizeOfEdgeValue);
					curstart += sizeOfEdgeValue;
					isstart++;
				}
				while(currentSequence < curvid){
					ch.write((byte)-1, 100);
					currentSequence++;
				}
				ch.write(entry, entry.length+100);
				currentSequence++;
				
			}


		}

		System.out.println(MAXID);
		File maxId = new File(adjDataPath+".maxid");
		BufferedWriter bw = new BufferedWriter(new FileWriter(maxId));
		bw.write(new String(MAXID+""));
		bw.flush();
		bw.close();

		// 处理顶点value
		initValue();

	}

	// 解析邻接表行，转换为合适的格式 vid-offset id-offset id-offset id-offset
	// vid,val : id,val->id,val->id,val
	private void extractLine(String line) {
		String vertexPart = null;
		String edgePart = null;
		StringTokenizer st = new StringTokenizer(line, vertexToEdgeSeparate);
		StringTokenizer est = null;
		int tokens = st.countTokens();

		vertexPart = st.nextToken(); // id:value
		if (tokens == 2)
			edgePart = st.nextToken(); // id,value->id,value->id,value

		if (edgePart != null) {
			est = new StringTokenizer(edgePart, edgeToEdgeSeparate);
		}

		int vid = Helper.getFirst(vidToValue(vertexPart));

		if (est != null) {
			while (est.hasMoreTokens()) {
				String p = est.nextToken();
				eidToValue(p, vid);
			}
		}
	}

	private long eidToValue(String part, int from) {

		StringTokenizer st = new StringTokenizer(part, idToValueSeparate);
		int to = -1;
		String token = null;

		if (st.countTokens() == 2) {
			to = Integer.parseInt(st.nextToken());
			token = st.nextToken();
			if (edgeProcessor != null) {
				try {
					this.addEdge(from, to, token);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else if (st.countTokens() == 1) {
			to = Integer.parseInt(st.nextToken());
			try {
				this.addEdge(from, to, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return Helper.pack(to, 0);
	}

	private long vidToValue(String part) {
		StringTokenizer st = new StringTokenizer(part, idToValueSeparate);
		int tokens = st.countTokens();
		int from = -1;
		String token = null;
		from = Integer.parseInt(st.nextToken());

		if (tokens == 2)
			token = st.nextToken();

		if (vertexProcessor != null) {
			VertexValueType value = vertexProcessor.receiveVertexValue(from,
					token);
			try {
				addVertexValue(from, value);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return Helper.pack(from, 0);

	}

	public void initValue() throws IOException {
		VertexValueType v = null;
		int sizeof = vertexValueTemplate.length;

		for (int i = 0; i <= MAXID; i++) {
			v = handler.initValue(i);
			if (cacheLineEnabled) {
				verterxValueTypeBytesToValueConverter.setValue(
						vertexValueTemplate, v);
				System.arraycopy(vertexValueTemplate, 0, cachelineTemplate, 0,
						sizeof);
				System.arraycopy(vertexValueTemplate, 0, cachelineTemplate,
						sizeof, sizeof);
				graphDataValueStream.write(cachelineTemplate);
			} else {
				verterxValueTypeBytesToValueConverter.setValue(
						vertexValueTemplate, v);
				graphDataValueStream.write(vertexValueTemplate);
				graphDataValueStream.write(vertexValueTemplate);
			}

			graphDataValueStream.flush();
		}
	}

	public static void main(String[] args) throws IOException {
		Handler<Float, EmptyType, Float> handler = new PageRankHandler();
		Graph<Float, EmptyType, Float> g = new Graph<>(null,
				new FloatConverter(), null, null, handler);
		g.preprocess();

		ChronicleHelper h = ChronicleHelper.newInstance();

		Object ob = h.read(637153);
		for (int i = 0; i <= MAXID; i++) {
			Object obj = h.read(i);
			if (obj instanceof byte[]) {
				byte[] arr = (byte[]) obj;
				System.out.println(arr.length/4);
				System.out.print(i + " : [ ");
				for (int k = 0; k < arr.length; k += 4) {
					int id = ((arr[k] & 0xff) << 24)
							+ ((arr[k + 1] & 0xff) << 16)
							+ ((arr[k + 2] & 0xff) << 8) + (arr[k + 3] & 0xff); // 计算出边的目的顶点
					System.out.print("," + id);
				}

				System.out.println("]");
			} else if (obj != null) {
				System.out.println(i + " " + obj);
			} else if (obj == null) {
				System.out.println(i + " null");
			}
		}

	}

}
