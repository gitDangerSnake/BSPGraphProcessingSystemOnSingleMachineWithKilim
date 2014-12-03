package edu.hnu.cg.graph.BytesToValueConverter;

public interface BytesToValueConverter<T> {
	public int sizeOf();
	public T getValue(byte[] array);
	public void setValue(byte[] array,T val);

}
