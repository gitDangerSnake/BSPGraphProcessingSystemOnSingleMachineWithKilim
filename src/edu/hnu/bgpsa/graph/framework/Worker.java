package edu.hnu.bgpsa.graph.framework;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
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
	protected static int MAXID;
	protected static MapperCore mc;
	private static boolean cacheLineEnabled;
	private static int cacheLineSize;
	private Set<Integer> breakPoints;
	public static BitSet firstMsgOrNot;

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
		numOfWorkers = config.getInt("nworkers");
		chronicle = ChronicleHelper.newInstance();
		String str = config.getStringValue("cacheLineEnabled");
		if (str.equals("true"))
			cacheLineEnabled = true;
		cacheLineSize = config.getInt("cachelineSize");

	}

	private int wid = counter++;

	private int sequence;

	private boolean hasMore() {
		return sequence <= MAXID;
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
		sequence = wid;
	}

	public Worker(Manager<V, E, M> mgr, BytesToValueConverter<V> v,
			BytesToValueConverter<E> e, BytesToValueConverter<M> m,
			Handler<V, E, M> h, int endite) {
		this.mgr = mgr;
		this.vTypeBytesToValueConverter = v;
		this.eTypeBytesToValueConverter = e;
		this.mTypeBytesToValueConverter = m;
		this.handler = h;
		sequence = wid;
		breakPoints = new TreeSet<Integer>();
		this.endIte = endite;

	}

	private Mailbox<Object> mailbox = new Mailbox<Object>(numOfWorkers/2,numOfWorkers);
	private Mailbox<Signal> siganlMailbox = new Mailbox<Signal>(3, 3);

	private volatile boolean iterationStart = true;

	class ComputeWorker extends Task {

		@SuppressWarnings("unchecked")
		@Override
		public void execute() throws Pausable, IOException {
			// 如果mailbox中有消息，那么就根据该消息对对应的vertex value进行计算
			Object msg = null;
			V oldVal = null;
			V newVal = null;

			while (computing) {

				msg = mailbox.get();
				// 如果msg是计算消息则，
				if (msg instanceof Message) {
					int to = ((Message) msg).getTo();
				

					if (handler.isCombinerProvided()) { // 如果combine钩子方法被提供，那么这里执行消息的combine操作

						M val = (M) ((Message) msg).getValue();
						long offset = mindex(to, 1);
						if (!firstMsgOrNot.get(to)) { // 如果是第一个消息，则无需合并，直接写入对应的位置
							writeMsgVal(offset, val);
							firstMsgOrNot.set(to);
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
						if (!firstMsgOrNot.get(to)) {
							newVal = handler.compute(oldVal, (Message) msg,
									currentIte, true);
							firstMsgOrNot.set(to);
						} else {
							newVal = handler.compute(oldVal, (Message) msg,
									currentIte, false);
						}

						if (!handler.isTwoValueEqual(oldVal, newVal)) {
							writeValue(offset, newVal);
							mgr.setUpdate(sequence);
						} else {
							mgr.setUnupdated(sequence);
						}
					}
					

				} else if ((msg instanceof Signal)
						&& (msg == Signal.MANAGER_ITERATION_OVER)) { // 如果消息不是计算消息，则进行其他的对应操作
					mgr.note(Signal.WORKER_COMPUTE_OVER);
				}else if((msg instanceof Signal) && (msg == Signal.WORKER_COMPUTE_OVER)){
					System.out.println("receive signal from my dispather...");
					break;
				}
				msg = null;
				oldVal = null;
				newVal = null;
			}
			
			mgr.note(Signal.WORKER_COMPUTE_OVER);
			
		}
	}

	private volatile int currentIte = 0;
	private int endIte;
	private volatile boolean computing = true;

	@Override
	public void execute() throws Pausable, IOException {
		int esize = eTypeBytesToValueConverter == null ? 0 : eTypeBytesToValueConverter.sizeOf(); //edge value 占用多少字节
		byte[] eTemp = new byte[esize];
		Message msg = null;
		byte[] arr = null;
		V val = null;
		// 获取到顶点数据，根据内容进行消息分发
		new ComputeWorker().start();
		while (computing) {
			Signal s = siganlMailbox.get();
			
			if (s == Signal.MANAGER_ITERATION_START) { //如果信号类型是开始迭代开始，则开始取数据，分发消息
				
				while(hasMore()){ //如果本序列还有数据则继续取数据
					if(!breakPoints.contains(sequence)){
						
						long valueOffset = vindex(sequence,0); //从内存映射文件中获取数据，并且获取的是PINGPANG 所指向的一列
						val = getValue(valueOffset);
						
						if(currentIte == 0 || mgr.isUpdated(sequence)){ //只有在上一个超级步中，如果该值发生了改变才会发送更新到其他顶点 或者是第一个超级步
							
							arr = next();//从CSR 数据文件中获取当前sequence所对应的数据
					
							if(arr!=null){//如果arr不为空则，表示取数据成功，进行消息分发
								int outdegree = arr.length/(4+esize); //计算出对应本sequence的顶点的出边度数
								if(eTypeBytesToValueConverter == null)
								for(int i=0;i<arr.length;i+=(4+esize)){ //逐个进行消息分发
									
									int vid = ((arr[i] & 0xff) << 24) + ((arr[i + 1] & 0xff) << 16) 
											+ ((arr[i + 2] & 0xff) << 8) + (arr[i + 3] & 0xff); // 计算出边的目的顶点
									if(eTypeBytesToValueConverter!= null){
										System.arraycopy(arr, i+4, eTemp, 0, esize);
									}
									
									//根据用户的需求生成消息
									if(eTypeBytesToValueConverter!= null){
										msg = handler.genMessage(sequence, vid, val, eTypeBytesToValueConverter.getValue(eTemp), outdegree);
									}else{
										msg = handler.genMessage(sequence, vid, val, null, outdegree);
									}
									
									if(msg!=null){
										int dest = locate(vid);
										mgr.send(dest, msg); //发送消息到目的worker
									}
									
									
								} //消息分发完了，准备获取下一个数据
								
							}//如果获得的数据为空，则表示该sequence对应的数据缺失,不进行处理
							
						}//如果当前sequence对应的顶点的value相对于上个迭代的值没有发生改变，则不需要发送消息
					}//如果在breakPoint集合中存在的sequence，表示该处的顶点数据缺失，不存在则无需去取数据,直接进行下一个
					
					//则在此处，sequence迁移至下一个应该被处理的数据位置
					sequenceIncrement();
				}//当不在hasMore的时候，表示在该迭代步骤内，本worker需要分发消息的数据已经处理完了，通知manager，告诉他分发完成的信号,并且重直sequence
				
				reset(); //重直sequence
				mgr.note(Signal.WORKER_DISPATCH_OVER);//通知manager分发完成
				
				
				
			} 
			else if (s == Signal.CHECK_STATE) {
				while ((s = siganlMailbox.getnb()) == null) {
					if (handler.isEqualsProvided()) {
						V o = getValue(vindex(sequence, 0));
						V n = getValue(vindex(sequence, 1));
						if (!handler.isTwoValueEqual(n, o)) {
							mgr.unactive(wid);
							reset();
							break;
						}
					} else {
						byte[] o = getRawValue(vindex(sequence, 0));
						byte[] n = getRawValue(vindex(sequence, 0));
						boolean same = Arrays.equals(o, n);
						if (!same) {
							mgr.unactive(wid);
							reset();
							break;
						}
					}
				}

			} 
			
			else if (s == Signal.OVER) { //如果消息是表示计算结束的，则将运行标记变成false
				computing = false;
			} else {
				unhandled(s); //否则，消息处理未定义
			}
				currentIte++; //迭代+1
		}
		
//		mgr.note(Signal.WORKER_DISPATCH_OVER);
//		mailbox.put(Signal.COMPUTE_OVER);

	}

	private void dispatch() throws IOException, Pausable {

		V oldVal = null;
		V newVal = null;

		while (hasMore()) {

			if (!breakPoints.contains(sequence)) {

				long valueOffset = vindex(sequence, 0);
				// System.out.println(Long.toHexString(valueOffset));
				V val = getValue(valueOffset);

				if (handler.isCombinerProvided()) {
					// 这里完成对消息和value的计算
					long msgValOffset = mindex(sequence, 0);
					oldVal = getValue(valueOffset);
					newVal = handler.computeWithCombine(oldVal,
							getMsgValue(msgValOffset));
					if (!handler.isTwoValueEqual(oldVal, newVal)) {
						writeValue(valueOffset, val);
						mgr.setUpdate(sequence);
					} else {
						mgr.setUnupdated(sequence);
					}
				}
				

				if (currentIte == 0 || mgr.isUpdated(sequence)) {
					int msize = eTypeBytesToValueConverter == null ? 0
							: eTypeBytesToValueConverter.sizeOf();
					byte[] array = next();
					if (array == null)
						continue;
					int outdegree = array.length / (4 + msize);
					byte[] valueTemp = new byte[msize];
					
					//debug
					StringBuilder sb = null;
					if(currentIte==1)
						sb = new StringBuilder();
					//debug

					for (int i = 0; i < array.length; i += 4) {
						int id = ((array[i] & 0xff) << 24)
								+ ((array[i + 1] & 0xff) << 16)
								+ ((array[i + 2] & 0xff) << 8)
								+ (array[i + 3] & 0xff);
						
					//debug
						if(currentIte==1)
							sb.append(id).append(",");
					//debug
						
						System.arraycopy(array, i + 4, valueTemp, 0, msize);
						Message msg = null;
						if (eTypeBytesToValueConverter != null)
							msg = handler.genMessage(sequence, id, val,
									eTypeBytesToValueConverter
											.getValue(valueTemp), outdegree);
						else
							msg = handler.genMessage(sequence, id, val, null,
									outdegree);

						if (msg != null) {
							int dest_worker = locate(id);
							mgr.send(dest_worker, msg);
						} else {
							// 通知manager这里没有message
						}
					}
					//debug
					if(currentIte==1)
						System.out.println("CSR: "+sequence +" ["+sb.toString()+"]");
					//debug

				}

				// 递增处理序号
				sequenceIncrement();
			}

		}

		reset();
		// 通知manager 分发完成
		mgr.note(Signal.WORKER_DISPATCH_OVER);
		System.out.println(wid + " notify manager dispatch over");

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
						// System.out.println(to+"*"
						// +cacheLineSize+"="+to*cacheLineSize+"--------"+Long.toHexString(to*cacheLineSize));
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
		mailbox.putnb(msg);
	}

	public void putSignal(Signal s) throws Pausable {
		siganlMailbox.put(s);
	}

	public void put(Signal s) throws Pausable {
		mailbox.put(s);
	}
}
