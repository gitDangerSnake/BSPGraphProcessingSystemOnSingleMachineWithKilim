package edu.hnu.cg.util.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;









import org.junit.Test;

import edu.hnu.cg.util.ChronicleHelper;

public class ChronicleHelperTest {

	class Task implements Runnable {

		int id;

		public Task(int id) {
			this.id = id;
		}

		public void run() {
			int degree = new Random().nextInt(10);
			for (int i = 0; i < degree; i++) {
				byte[] ret = (byte[])ChronicleHelper.newInstance().read(id);
				System.out.println(Arrays.toString(ret));
			}

			System.out.println(Thread.currentThread().getName() + " Time taken: " + (System.currentTimeMillis() - start));

		}

	}

	private long start;
	private long end;

	@Test
	public void test() throws IOException, InterruptedException {

		int availableProcessor = Runtime.getRuntime().availableProcessors();

		/*for (int i = 0; i < availableProcessor * 2; i++) {
			
			byte[] data = new byte[i+1];
        	for(int k=0;k<i+1;k++)
        		data[k] = (byte)k;
			
			ChronicleHelper.newInstance().write(data,100);
		}*/

		start = System.currentTimeMillis();
		for (int i = 0; i < availableProcessor * 2; i++) {
			new Thread(new Task(i)).start();
		}
		

		Thread.sleep(5000);

	}
	
	@Test
	public void instanceoftest(){
		byte[] msg = new byte[1024];
		System.out.println(msg instanceof byte[]);
		for(int i=1;i<2048;i++){
			System.out.println(i&(1023-1));
		}
	}
	
	@Test
	public void testWriteNull(){
		ChronicleHelper.newInstance().write("Hello1", 100);
		ChronicleHelper.newInstance().write((byte)-1, 100);
		ChronicleHelper.newInstance().write("hello2", 100);
		/*String str = (String)ChronicleHelper.newInstance().read(0);
		System.out.println(str);
		*/
		Object arr = ChronicleHelper.newInstance().read(1);
		System.out.println(arr instanceof byte[]);
		String str1 = (String)ChronicleHelper.newInstance().read(0);
		System.out.println(str1);
		
		
	}
	
	

}
