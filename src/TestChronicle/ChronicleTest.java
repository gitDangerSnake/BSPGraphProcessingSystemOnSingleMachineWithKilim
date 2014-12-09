package TestChronicle;

import edu.hnu.bgpsa.graph.framework.Signal;
import edu.hnu.cg.util.ChronicleHelper;

public class ChronicleTest {

	public static void main(String[] args) {
		for(int i=0;i<2048;i++){
			new Reader(i).start();
		}
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
