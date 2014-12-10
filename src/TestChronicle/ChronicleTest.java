package TestChronicle;

import edu.hnu.bgpsa.graph.framework.Signal;
import edu.hnu.cg.graph.BytesToValueConverter.FloatConverter;
import edu.hnu.cg.util.ChronicleHelper;
import edu.hnu.cg.util.Helper;

public class ChronicleTest {

	public static void main(String[] args) {
		byte[] parcel = new byte[8];
		int to = 345;
		byte[] v = Helper.intToByteArray(to);
		float f = 3.14f;
		byte[] fl = new byte[4];
		new FloatConverter().setValue(fl, f	);
		System.arraycopy(v, 0, parcel, 0, 4);
		System.arraycopy(fl, 0, parcel, 4, 4);
		
		
		
	}
	
	public static long pack(int a, float b) {
		return (long) (((long) a << 32) + b);
	}

	public static int getFirst(long e) {
		return (int) (e >> 32);
	}

	public static int getSecond(long e) {
		return (int) (e & 0x00000000ffffffffL);
	}

}

class Reader extends Thread{
	
	private static ChronicleHelper h = ChronicleHelper.newInstance();
	
	int id;
	int sequence;
	public Reader(int id){
		this.id = id;
		sequence = id;
	}
	
	public boolean hasMore(){
		return sequence <= 4847570;
	}
	
	public void increment(){
		sequence += 2048;
	}
	
	public void run(){
		
		while(hasMore()){
			Object obj = h.read(sequence);
			System.out.println(obj);
			increment();
		}
		
		
	}
}
