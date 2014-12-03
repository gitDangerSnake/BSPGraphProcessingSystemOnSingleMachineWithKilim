package edu.hnu.bgpsa.graph.framework;

import java.io.IOException;
import java.util.BitSet;

import kilim.Mailbox;
import kilim.Pausable;
import kilim.Task;
import edu.hnu.bgpsa.graph.Exception.WrongNumberOfWorkersException;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.MapperCore;
import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.config.Configure;
import edu.hnu.cg.graph.preprocessing.EdgeProcessor;
import edu.hnu.cg.graph.preprocessing.VertexProcessor;

public class Manager<V, E, M> extends Task {

	// /////////configure variables///
	private static Configure config;
	private static String baseFilePath;
	private static String format;
	private static String adjDataPath;
	private static int nvertices;
	private static int MAXID;

	private static int nworkers;
	private static int cachelineSize;
	private static int vertexIdBytesLength;
	private static int lengthBytesLength;
	private static int degreeBytesLength;
	private static int valueOffsetWidth;

	// global moniter flags
	private static BitSet bits;
	private BitSet workerBit;

	static {
		config = Configure.getConfigure(); // get configure

		// get configurations
		baseFilePath = config.getStringValue("baseFilePath");
		format = config.getStringValue("format");
		adjDataPath = config.getStringValue("adjDataPath");
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
		vertexIdBytesLength = config.getInt("vertexIdBytesLength");
		lengthBytesLength = config.getInt("lengthBytesLength");
		degreeBytesLength = config.getInt("degreeBytesLength");
		valueOffsetWidth = config.getInt("valueOffsetWidth");

		bits = new BitSet(MAXID);
		bits.set(0, MAXID);
	}

	private Mailbox<Signal> mailbox;
//	private Mailbox<Signal> checkbox;

	private Worker<V, E, M>[] workers = (Worker<V, E, M>[]) new Worker[nworkers];

	private BytesToValueConverter<V> VTypeBytesToValueConverter;
	private BytesToValueConverter<E> ETypeBytesToValueConverter;
	private BytesToValueConverter<M> MTypeBytesToValueConverter;
	private int currIte;
	private int endIte;
	private Handler<V, E, M> handler;

	// private MapperCore mc;

	public Manager(BytesToValueConverter<V> VTypeBytesToValueConverter,
			BytesToValueConverter<E> ETypeBytesToValueConverter,
			BytesToValueConverter<M> MTypeBytesToValueConverter,
			Handler<V, E, M> handler) throws IOException {
		// 初始化worker
		this.VTypeBytesToValueConverter = VTypeBytesToValueConverter;
		this.ETypeBytesToValueConverter = ETypeBytesToValueConverter;
		this.MTypeBytesToValueConverter = MTypeBytesToValueConverter;
		this.handler = handler;
		mailbox = new Mailbox<Signal>(nworkers, nworkers);
//		checkbox = new Mailbox<Signal>(nworkers, nworkers);
		workerBit = new BitSet(nworkers);

	}

	@Override
	public void execute() throws Pausable {
		// 载入数据
		// 启动worker
		startWorkers();
		new Monitor().start();
		while (currIte < endIte) {
			activeWorkers();

			while (!monitorDispatchOver) {
				// do some work
				// logging or 预加载数据等等
			}

			monitorDispatchOver = false;
			interfer();
			while (!monitorComputeOver) {
				// do some work here
				// logging
			}

			monitorComputeOver = false;
			Worker.PingPang();

			if (convergence()) {
				break;
			}
//			workerBit.clear();

		}

		terminate();

	}

	private void terminate() throws Pausable {
		for (int i = 0; i < nworkers; i++) {
			workers[i].putSignal(Signal.OVER);
		}

	}

	public boolean convergence() throws Pausable {
		/*// 检查是否收敛
		for (int i = 0; i < nworkers; i++) {
			workers[i].putSignal(Signal.CHECK_STATE);
		}

		Signal s = null;
		int count = 0;
		while (workerBit.isEmpty()) {
			s = checkbox.getnb();
			if (s != null && s == Signal.ITERATION_UNACTIVE) {
				count++;
				if (count == nworkers) {
					return true;
				}
			}
		}

		for (int i = 0; i < nworkers; i++) {
			workers[i].putSignal(Signal.CHECK_STATE_OVER);
		}

		*//** TO DO : 修改kilim代码添加邮箱clear处理器 **//*
		// 处理checkbox中残余的消息

		return false;*/
		
		return bits.isEmpty();
	}

	private void activeWorkers() throws Pausable {
		for (int i = 0; i < nworkers; i++) {
			workers[i].putSignal(Signal.ITERATION_START);
		}
	}

	private void interfer() throws Pausable {
		for (int i = 0; i < nworkers; i++) {
			workers[i].put(Signal.ITERATION_OVER);
		}
	}

	private volatile boolean monitorDispatchOver = false;
	private volatile boolean monitorComputeOver = false;

	enum MonitorType {
		dispatch_over_event, compute_over_event
	}

	class Monitor extends Task {
		private int count = 0;

		@Override
		public void execute() throws Pausable {
			Signal s = null;
			while ((s = mailbox.get()) != null) {
				if (s == Signal.DISPATCH_OVER) {
					count++;
					if (count == nworkers) {
						count = 0;
						monitorDispatchOver = true;
					}
				} else if (s == Signal.COMPUTE_OVER) {
					count++;
					if (count == nworkers) {
						count = 0;
						monitorComputeOver = true;
					}
				}
			}
		}

	}

	private void startWorkers() throws Pausable {
		for (int i = 0; i < nworkers; i++) {
			workers[i] = new Worker<V,E,M>(this, VTypeBytesToValueConverter,
					ETypeBytesToValueConverter, MTypeBytesToValueConverter,
					handler);
			workers[i].start();
		}
	}

	public void send(int dest_worker, Message msg) throws Pausable {
		workers[dest_worker].put(msg);
	}

	public void note(Signal s) throws Pausable {
		mailbox.put(s);
	}

	public boolean check(int to) {
		return bits.get(to);
	}

	public void unactive(int id) {
		workerBit.set(id);
	}

	public void active(int id) {
		workerBit.clear(id);
	}

	public void setUpdate(int sequence) {
		bits.set(sequence);
		
	}

	public boolean isUpdated(int sequence) {
		return bits.get(sequence);
	}

	public void setUnupdated(int sequence) {
		bits.clear(sequence);
	}

}

enum Signal {
	ITERATION_START, ITERATION_OVER, CHECK_STATE, CHECK_STATE_OVER, DISPATCH_OVER, COMPUTE_OVER, ITERATION_UNACTIVE, ITERATION_ACTIVE, OVER

}
