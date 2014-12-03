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

import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.BytesToValueConverter.MsgBytesTovalueConverter;
import edu.hnu.cg.graph.config.Configure;
import edu.hnu.cg.graph.preprocessing.EdgeProcessor;
import edu.hnu.cg.graph.preprocessing.VertexProcessor;
import edu.hnu.cg.util.BufferedDataInputStream;
import edu.hnu.cg.util.ChronicleHelper;
import edu.hnu.cg.util.Helper;

public class Graph<VertexValueType, EdgeValueType, MsgValueType> {

	private static Logger logger;
	private static Configure config = Configure.getConfigure();

	public static byte[] cachelineTemplate;
	public static int[] lengthsOfWorkerMsgsPool;

	private static int valueOffsetWidth;
	private static int vertexIdBytesLength;
	private static int lengthBytesLength;
	private static int degreeBytesLength;

	private static final String vertexToEdgeSeparate = ":";
	private static final String idToValueSeparate = ",";
	private static final String edgeToEdgeSeparate = "->";

	static {

		vertexIdBytesLength = config.getInt("vertexIdBytesLength");
		degreeBytesLength = config.getInt("degreeBytesLength");
		valueOffsetWidth = config.getInt("valueOffsetWidth");
		lengthBytesLength = config.getInt("lengthBytesLength");

		cachelineTemplate = new byte[config.getInt("cachelineSize")];
		lengthsOfWorkerMsgsPool = new int[config.getInt("workers")];

		logger = Logger.getLogger("Graph PreProcessing");
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
	 * 文件内容的格式
	 */
	private graphFormat format;
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

	/*
	 * 数据转换器：字节数组转换为数据MsgValueType
	 */
	private MsgBytesTovalueConverter<MsgValueType> msgValueTypeBytesToValueConverter;

	/*
	 * 默认的VertexValueType字节缓存数组
	 */
	private byte[] vertexValueTemplate;

	/*
	 * 默认的EdgeValueType字节缓存数组
	 */
	private byte[] edgeValueTemplate;
	/*
	 * 默认的MsgValueType字节缓存数组
	 */
	private byte[] msgTemplate;

	private DataOutputStream shovelWriter;
	private DataOutputStream shovelValueWriter;

	private DataOutputStream graphDataStream;
	private DataOutputStream graphDataValueStream;
	private DataOutputStream graphDataIndexStream;

	public Graph(
			String graphFilename,
			String format,
			long numEdges,
			long numVertices,
			VertexProcessor<VertexValueType> vertexProcessor,
			EdgeProcessor<EdgeValueType> edgeProcessor,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter,
			MsgBytesTovalueConverter<MsgValueType> msgValueTypeBytesToValueConverter,
			byte[] vertexValueTemplate, byte[] edgeValueTemplate,
			byte[] msgTemplate, DataOutputStream shovelWriter,
			DataOutputStream shovelValueWriter,
			DataOutputStream graphDataStream,
			DataOutputStream graphDataValueStream) {

		this.graphFilename = graphFilename;

		if (format.toLowerCase().equals("edgelist"))
			this.format = graphFormat.EDGELIST;
		else if (format.toLowerCase().equals("adjacency"))
			this.format = graphFormat.ADJACENCY;

		this.numEdges = numEdges;
		this.numVertices = numVertices;

		this.vertexProcessor = vertexProcessor;
		this.edgeProcessor = edgeProcessor;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		this.verterxValueTypeBytesToValueConverter = verterxValueTypeBytesToValueConverter;
		this.msgValueTypeBytesToValueConverter = msgValueTypeBytesToValueConverter;

		this.vertexValueTemplate = vertexValueTemplate;
		this.edgeValueTemplate = edgeValueTemplate;
		this.msgTemplate = msgTemplate;
		this.shovelWriter = shovelWriter;
		this.shovelValueWriter = shovelValueWriter;
		this.graphDataStream = graphDataStream;
		this.graphDataValueStream = graphDataValueStream;
	}

	public Graph(
			String graphFilename,
			String format,
			long numEdges,
			long numVertices,
			VertexProcessor<VertexValueType> vertexProcessor,
			EdgeProcessor<EdgeValueType> edgeProcessor,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter,
			MsgBytesTovalueConverter<MsgValueType> msgValueTypeBytesToValueConverter)
			throws FileNotFoundException {
		this.graphFilename = graphFilename;

		if (format.toLowerCase().equals("edgelist"))
			this.format = graphFormat.EDGELIST;
		else if (format.toLowerCase().equals("adjacency"))
			this.format = graphFormat.ADJACENCY;

		this.numEdges = numEdges;
		this.numVertices = numVertices;
		this.vertexProcessor = vertexProcessor;
		this.edgeProcessor = edgeProcessor;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		this.verterxValueTypeBytesToValueConverter = verterxValueTypeBytesToValueConverter;
		this.msgValueTypeBytesToValueConverter = msgValueTypeBytesToValueConverter;

		this.shovelWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.shovelFilename(graphFilename))));
		this.shovelValueWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(
						Filename.shovelValueFilename(graphFilename))));

		this.graphDataStream = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.adjFilename(graphFilename))));
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

		if (msgValueTypeBytesToValueConverter != null) {
			msgTemplate = new byte[msgValueTypeBytesToValueConverter.sizeOf()];
		} else {
			msgTemplate = new byte[0];
		}

	}

	public Graph(
			String filename,
			String _format,
			BytesToValueConverter<EdgeValueType> _edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> _verterxValueTypeBytesToValueConverter,
			MsgBytesTovalueConverter<MsgValueType> _msgValueTypeBytesToValueConverter,
			VertexProcessor<VertexValueType> _vertexProcessor,
			EdgeProcessor<EdgeValueType> _edgeProcessor)
			throws FileNotFoundException {

		graphFilename = filename;

		if (_format.toLowerCase().equals("edgelist"))
			format = graphFormat.EDGELIST;
		else if (_format.toLowerCase().equals("adjacency"))
			format = graphFormat.ADJACENCY;

		vertexProcessor = _vertexProcessor;
		edgeProcessor = _edgeProcessor;
		edgeValueTypeBytesToValueConverter = _edgeValueTypeBytesToValueConverter;
		verterxValueTypeBytesToValueConverter = _verterxValueTypeBytesToValueConverter;
		msgValueTypeBytesToValueConverter = _msgValueTypeBytesToValueConverter;

		this.shovelWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.shovelFilename(graphFilename))));
		this.shovelValueWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(
						Filename.shovelValueFilename(graphFilename))));

		this.graphDataStream = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.adjFilename(graphFilename))));
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

		if (msgValueTypeBytesToValueConverter != null) {
			msgTemplate = new byte[msgValueTypeBytesToValueConverter.sizeOf()];
		} else {
			msgTemplate = new byte[0];
		}

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
		if (format == graphFormat.EDGELIST) {
			Pattern p = Pattern.compile("\\s");
			while ((ln = bReader.readLine()) != null) {
				numEdges++;
				if (lnNum % 1000000 == 0) {
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

		} else if (format == graphFormat.ADJACENCY) {
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
		int cachelineSize = config.getInt("cachelineSize");

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
					for(int p = isstart ; p < s;p++){
						int to = Helper.getSecond(edges[p]);
						System.arraycopy(Helper.intToByteArray(to), 0, entry, curstart, 4);
						curstart += 4;
						System.arraycopy(edgeValues, p*sizeOfEdgeValue, entry, curstart, sizeOfEdgeValue);
						curstart += sizeOfEdgeValue;
					}
					
					ch.write(entry, 100+entry.length);
					curvid = from;
					isstart = s;
				}
			} else {
				
				while(curvid > currentSequence){
					ch.write((byte)0x11, 100);
					currentSequence++;
				}

			}

		}

	}

	public void adj_process(String line) {

		StringTokenizer st = new StringTokenizer(line, vertexToEdgeSeparate);
		int tokens = st.countTokens();

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

	public static void main(String[] args) throws IOException {

	}

	public void close() {
	}

}
