package edu.hnu.cg.graph;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.hnu.bgpsa.app.component.ComponentHandler;
import edu.hnu.bgpsa.app.component.EmptyType;
import edu.hnu.bgpsa.graph.Exception.WrongNumberOfWorkersException;
import edu.hnu.bgpsa.graph.framework.Handler;
import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
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
	
	public static int getMaxID(){
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
			DataOutputStream shovelWriter,
			DataOutputStream shovelValueWriter,
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
		BufferedReader bReader = new BufferedReader(new FileReader((new File(
				graphFilename))));
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
							Integer.parseInt(tokenStrings[1]), tokenStrings[2]);
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
		

	}

	public void addEdge(int from, int to, String token) throws IOException {
		if (from == to) {
			if (vertexProcessor != null) {
				VertexValueType value = vertexProcessor.receiveVertexValue(
						from, token);
				try {
					addVertexValue(from, value);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		addToShovel(
				from,
				to,
				(edgeProcessor != null ? edgeProcessor.receiveEdge(from, to,
						token) : null));
	}

	private void addToShovel(int from, int to, EdgeValueType value)
			throws IOException {
		shovelWriter.writeLong(Helper.pack(from, to));
		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTypeBytesToValueConverter.setValue(edgeValueTemplate,
					value);
			shovelWriter.write(edgeValueTemplate);
		}
	}

	public void addVertexValue(int from, VertexValueType value)
			throws IOException {
		shovelValueWriter.writeLong(Helper.pack(0, from));
		verterxValueTypeBytesToValueConverter.setValue(vertexValueTemplate,
				value);
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
		int curvid = 0;
		int currentSequence = 0;
		int isstart = 0;

		byte[] entry = null;
		ChronicleHelper ch = ChronicleHelper.newInstance();
		// 从边构建邻接表
		for (int s = 0; s < edges.length; s++) {
			int from = Helper.getFirst(edges[s]);

			if (currentSequence == curvid) {
				if (from != curvid) {
					int count = s - isstart;
					entry = new byte[count * (4 + sizeOfEdgeValue)];
					int curstart = 0;
					for (int p = isstart; p < s; p++) {
						int to = Helper.getSecond(edges[p]);
						if(to > MAXID) MAXID = to;
						System.arraycopy(Helper.intToByteArray(to), 0, entry,
								curstart, 4);
						curstart += 4;
						System.arraycopy(edgeValues, p * sizeOfEdgeValue,
								entry, curstart, sizeOfEdgeValue);
						curstart += sizeOfEdgeValue;
					}

					ch.write(entry, 100 + entry.length);
					curvid = from;
					if(curvid - currentSequence == 1)
						++currentSequence;
					isstart = s;
					if(from > MAXID) MAXID = from;
				}
			} else {

				while (curvid > currentSequence) {
					ch.write((byte) 0x11, 100);
					currentSequence++;
				}

			}

		}
		
		System.out.println(MAXID);
		
		//处理顶点value
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

	public  void initValue() throws IOException {
		VertexValueType v = null;
		int sizeof = vertexValueTemplate.length;
		
		for(int i=0;i<=MAXID;i++){
			v = handler.initValue(i);
			if(cacheLineEnabled){
				verterxValueTypeBytesToValueConverter.setValue(vertexValueTemplate, v);
				System.arraycopy(vertexValueTemplate,0,cachelineTemplate,0,sizeof);
				System.arraycopy(vertexValueTemplate,0,cachelineTemplate,sizeof,sizeof);
				graphDataValueStream.write(cachelineTemplate);
			}else{
				verterxValueTypeBytesToValueConverter.setValue(vertexValueTemplate, v);
				graphDataValueStream.write(vertexValueTemplate);
				graphDataValueStream.write(vertexValueTemplate);
			}
			
			graphDataValueStream.flush();
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		Handler<Integer,EmptyType,Integer> handler = new ComponentHandler();
		Graph<Integer,EmptyType,Integer> g = new Graph<Integer,EmptyType,Integer>(null, new IntConverter(),  null, null, handler);
		g.initValue();
		
		ChronicleHelper chronicleHelper = ChronicleHelper.newInstance();
		
		byte[] data = (byte[]) chronicleHelper.read(0);
		System.out.println(Arrays.toString(data));
		for(int i=0;i<data.length;i+=4){
			int id = ((data[i] & 0xff) << 24)
					+ ((data[i + 1] & 0xff) << 16)
					+ ((data[i + 2] & 0xff) << 8)
					+ (data[i + 3] & 0xff);
			System.out.println(id);
		}
		

	}
	

}
