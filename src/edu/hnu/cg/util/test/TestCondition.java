package edu.hnu.cg.util.test;

import java.util.BitSet;

public class TestCondition {

	public static void main(String[] args) throws InterruptedException {

		Thread[] ts = new Thread[100];
		for(int i=0;i<100;i++){
			ts[i] = new tttt(i);
			ts[i].start();
		}
		
		BitSet bs = tttt.h;
		System.out.println(bs.toString());
		for(int i=0;i<100;i++){
			if(bs.get(i)) System.out.print(1+" ");
			else System.out.print(0+" ");
		}
		
	}
	
	
	

}
class tttt extends Thread{
	static BitSet h = new BitSet(100);
	int id;
	public tttt(int id){
		this.id =id;
	}
	public void run(){
		if(id % 2 == 0) h.set(id);
	}
}