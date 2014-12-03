package edu.hnu.cg.graph.BytesToValueConverter;

public class IntConverter implements BytesToValueConverter<Integer> {

	@Override
	public int sizeOf() {
		return 4;
	}

	@Override
	public Integer getValue(byte[] array) {
		return ((array[0] & 0xff) << 24) + ((array[1] & 0xff) << 16) + ((array[2] & 0xff) << 8) + (array[3] & 0xff);
	}

	@Override
	public void setValue(byte[] array, Integer val) {
		array[3] = (byte) ((val) & 0xff);
		array[2] = (byte) ((val >>> 8) & 0xff);
		array[1] = (byte) ((val >>> 16) & 0xff);
		array[0] = (byte) ((val >>> 24) & 0xff);
	}

}
