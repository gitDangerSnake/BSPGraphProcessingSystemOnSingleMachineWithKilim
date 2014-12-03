package edu.hnu.bgpsa.graph.framework;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import edu.hnu.cg.graph.MapperCore;
import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.config.Configure;
import edu.hnu.cg.util.ChronicleHelper;
import kilim.Mailbox;
import kilim.Pausable;
import kilim.Task;

public class Worker<V, E, M> extends Task {

	private static int counter;
	private static ChronicleHelper chronicle;
	private static int numOfWorkers;
	private static int MAXID;
	private static MapperCore mc;
	private static boolean cacheLineEnabled;
	private static int cacheLineSize;
	private Set<Integer> breakPoints;

	private static boolean PINGPANG = true;

	static void PingPang() {
		PINGPANG = !PINGPANG;
	}

	private Manager<V, E, M> mgr;
	private Handler<V, E, M> handler;

	private BytesToValueConverter<V> vTypeBytesToValueConverter;
	private BytesToValueConverter<E> eTypeBytesToValueConverter;
	private BytesToValueConverter<M> mTypeBytesToValueConverter;

	static {
		Configure config = Configure.getConfigure();
		MAXID = config.getInt("maxId");
		numOfWorkers = config.getInt("nworkers");
		chronicle = ChronicleHelper.newInstance();
		String str = config.getStringValue("cacheLineEnabled");
		if (str.equals("true"))
			cacheLineEnabled = true;
		cacheLineSize = config.getInt("cachelineSize");

		// 初始化mappercore
	}

	private int id = counter++;

	private int sequence;

	private boolean hasMore() {
		return (sequence + counter) < MAXID;
	}

	private byte[] next() {
		Object eturn = chronicle.read(sequence);
		if (eturn instanceof byte[]) {
			return (byte[]) eturn;
		}

		breakPoints.add(sequence);
		sequenceIncrement();
		return null;
	}

	private void sequenceIncrement() {
		sequence += counter;
	}

	private void reset() {
		sequence = id;
	}

	public Worker(Manager<V, E, M> mgr, BytesToValueConverter<V> v,
			BytesToValueConverter<E> e, BytesToValueConverter<M> m,
			Handler<V, E, M> h) {
		this.mgr = mgr;
		this.vTypeBytesToValueConverter = v;
		this.eTypeBytesToValueConverter = e;
		this.mTypeBytesToValueConverter = m;
		this.handler = h;
		sequence = id;
		breakPoints = new TreeSet<Integer>();

	}

	private Mailbox<Object> mailbox = new Mailbox<Object>(128, numOfWorkers);
	private Mailbox<Signal> siganlMailbox = new Mailbox<Signal>(3, 3);

	class ComputeWorker extends Task {

		@SuppressWarnings("unchecked")
		@Override
		public void execute() throws Pausable, IOException {
			// 如果mailbox中有消息，那么就根据该消息对对应的vertex value进行计算
			Object msg = null;
			V oldVal = null;
			V newVal = null;
			boolean isFirstMsg = true;

			while (currentIte < endIte && computing) {
				msg = mailbox.get();
				// 如果msg是计算消息则，
				if (msg instanceof Message) {
					int to = ((Message) msg).getTo();

					if (handler.isCombinerProvided()) { // 如果combine钩子方法被提供，那么这里执行消息的combine操作

						M val = (M) ((Message) msg).getValue();
						long offset = mindex(to, 1);
						if (isFirstMsg) { // 如果是第一个消息，则无需合并，直接写入对应的位置
							writeMsgVal(offset, val);
							isFirstMsg = false;
						} else { // 表明该消息位不是第一个消息，所以要和之前的结果进行合并操作
							M oldMsgVal = getMsgValue(offset);
							M newMsgVal = handler.combine(oldMsgVal, val);
							writeMsgVal(offset, newMsgVal);
							oldMsgVal = null;
							newMsgVal = null;
						}
						val = null;

					} else {// 否则这里执行计算操作
						long offset = vindex(to, 1);
						oldVal = getValue(offset);
						newVal = handler.compute(oldVal, (Message) msg,
								currentIte);
						if (!handler.isTwoValueEqual(oldVal, newVal)) {
							writeValue(offset, newVal);
							mgr.setUpdate(sequence);
						} else {
							mgr.setUnupdated(sequence);
						}
					}

				} else if ((msg instanceof Signal)
						&& (msg == Signal.ITERATION_OVER)) { // 如果消息不是计算消息，则进行其他的对应操作
					mgr.note(Signal.COMPUTE_OVER);
				}
				msg = null;
				oldVal = null;
				newVal = null;
			}
		}
	}

	private int currentIte = 0;
	private int endIte;
	private volatile boolean computing = true;

	@Override
	public void execute() throws Pausable, IOException {
		// 获取到顶点数据，根据内容进行消息分发
		new ComputeWorker().start();
		while (currentIte < endIte && computing) {
			Signal s = siganlMailbox.get();
			if (s == Signal.ITERATION_START) {
				dispatch();
			} else if (s == Signal.CHECK_STATE) {
				while ((s = siganlMailbox.getnb()) == null) {
					if (handler.isEqualsProvided()) {
						V o = getValue(vindex(sequence, 0));
						V n = getValue(vindex(sequence, 1));
						if (!handler.isTwoValueEqual(n, o)) {
							mgr.unactive(id);
							reset();
							break;
						}
					} else {
						byte[] o = getRawValue(vindex(sequence, 0));
						byte[] n = getRawValue(vindex(sequence, 0));
						boolean same = Arrays.equals(o, n);
						if (!same) {
							mgr.unactive(id);
							reset();
							break;
						}
					}
				}
				if (s == Signal.CHECK_STATE_OVER) {

				}

			}
		}
	}

	private void dispatch() throws IOException, Pausable {

		V oldVal = null;
		V newVal = null;
		while (hasMore()) {

			if (!breakPoints.contains(sequence)) {
				
				long valueOffset = vindex(sequence, 0);
				V val = getValue(valueOffset);

				if (handler.isCombinerProvided()) {
					// 这里完成对消息和value的计算
					long msgValOffset = mindex(sequence, 0);
					oldVal = getValue(valueOffset);
					newVal = handler.compute(oldVal, getMsgValue(msgValOffset));
					if (!handler.isTwoValueEqual(oldVal, newVal)) {
						writeValue(valueOffset, val);
						mgr.setUpdate(sequence);
					} else {
						mgr.setUnupdated(sequence);
					}
				}

				if (mgr.isUpdated(sequence)) {
					int msize = eTypeBytesToValueConverter.sizeOf();
					byte[] array = next();
					int outdegree = array.length / (4 + msize);
					byte[] valueTemp = new byte[msize];

					for (int i = 0; i < outdegree; i++) {
						int id = ((array[i] & 0xff) << 24)
								+ ((array[i + 1] & 0xff) << 16)
								+ ((array[i + 2] & 0xff) << 8)
								+ (array[i + 3] & 0xff);
						System.arraycopy(array, i + 4, valueTemp, 0, msize);
						Message msg = handler.genMessage(sequence, id, val,
								eTypeBytesToValueConverter.getValue(valueTemp));
						if (msg != null) {
							int dest_worker = locate(id);
							mgr.send(dest_worker, msg);
						} else {
							// 通知manager这里没有message
						}
					}

				}

				// 递增处理序号
				sequenceIncrement();

			}

			reset();
			// 通知manager 分发完成
			mgr.note(Signal.DISPATCH_OVER);
			Signal s = siganlMailbox.get();
			if (s == Signal.ITERATION_START) {
				currentIte++;
			} else if (s == Signal.OVER) {
				computing = false;
			} else {
				unhandled(s);
			}
		}
	}

	private void unhandled(Signal s) {

	}

	/*
	 * 在这里计算的时候，我们假设每一个记录的长度不超过一个cacheline的长度 如果type==0，表示取值是PINGPANG 所指的一列
	 */
	private long vindex(int to, int type) {
		if (handler.isCombinerProvided()) {
			if (cacheLineEnabled) {
				return to * cacheLineSize;
			} else {
				return to
						* (vTypeBytesToValueConverter.sizeOf() + mTypeBytesToValueConverter
								.sizeOf() * 2);
			}
		} else {
			if (cacheLineEnabled) {
				if (PINGPANG) {
					if (type == 0) {
						return to * cacheLineSize;
					}
					return to * cacheLineSize
							+ vTypeBytesToValueConverter.sizeOf();
				} else {
					if (type == 0) {
						if (type == 0)
							return to * cacheLineSize
									+ vTypeBytesToValueConverter.sizeOf();
					}
					return to * cacheLineSize;
				}

			} else {

				int sizeOfValue = vTypeBytesToValueConverter.sizeOf();
				if (PINGPANG) {
					if (type == 0) {
						return to * sizeOfValue * 2;
					}

					return (sizeOfValue * 2) * to + sizeOfValue;
				} else {
					if (type == 0) {
						return (sizeOfValue * 2) * to + sizeOfValue;
					}
					return to * sizeOfValue * 2;

				}
			}
		}
	}

	private long mindex(int to, int type) {
		if (cacheLineEnabled) {
			if (PINGPANG) {
				if (type == 0) {
					return to * cacheLineSize
							+ vTypeBytesToValueConverter.sizeOf();
				}
				return to * cacheLineSize + vTypeBytesToValueConverter.sizeOf()
						+ mTypeBytesToValueConverter.sizeOf();
			} else {
				if (type == 0) {
					return to * cacheLineSize
							+ vTypeBytesToValueConverter.sizeOf()
							+ mTypeBytesToValueConverter.sizeOf();
				}
				return to * cacheLineSize + vTypeBytesToValueConverter.sizeOf();
			}
		} else {
			int sizeOfMsgVal = mTypeBytesToValueConverter.sizeOf();
			int sizeOfValue = vTypeBytesToValueConverter.sizeOf();
			if (PINGPANG) {
				if (type == 0) {
					return to * (2 * sizeOfMsgVal + sizeOfValue) + sizeOfValue;
				}

				return to * (2 * sizeOfMsgVal + sizeOfValue) + sizeOfValue
						+ sizeOfMsgVal;
			} else {
				if (type == 0) {
					return to * (2 * sizeOfMsgVal + sizeOfValue) + sizeOfValue
							+ sizeOfMsgVal;
				}

				return to * (2 * sizeOfMsgVal + sizeOfValue) + sizeOfValue;
			}
		}

	}

	public int locate(int n) {
		return n & (counter - 1);
	}

	public V getValue(long offset) throws IOException {

		return vTypeBytesToValueConverter.getValue(mc.get(offset,
				vTypeBytesToValueConverter.sizeOf()));
	}

	public byte[] getRawValue(long offset) throws IOException {
		return mc.get(offset, vTypeBytesToValueConverter.sizeOf());
	}

	private M getMsgValue(long offset) throws IOException {
		return mTypeBytesToValueConverter.getValue(mc.get(offset,
				mTypeBytesToValueConverter.sizeOf()));

	}

	public void writeValue(long offset, V val) throws IOException {
		byte[] array = new byte[vTypeBytesToValueConverter.sizeOf()];
		vTypeBytesToValueConverter.setValue(array, val);
		mc.put(offset, array);
	}

	public void writeMsgVal(long offset, M msgVal) throws IOException {
		byte[] array = new byte[mTypeBytesToValueConverter.sizeOf()];
		mTypeBytesToValueConverter.setValue(array, msgVal);
		mc.put(offset, array);
	}

	public void put(Message msg) throws Pausable {
		mailbox.put(msg);
	}

	public void putSignal(Signal s) throws Pausable {
		siganlMailbox.put(s);
	}

	public void put(Signal s) throws Pausable {
		mailbox.put(s);
	}
}
