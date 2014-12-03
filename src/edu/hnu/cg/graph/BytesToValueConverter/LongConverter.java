package edu.hnu.cg.graph.BytesToValueConverter;

public class LongConverter implements BytesToValueConverter<Long> {

	@Override
	public int sizeOf() {
		return 8;
	}

	@Override
	public Long getValue(byte[] array) {
		return ((long) (array[0] & 0xff) << 56) + ((long) (array[1] & 0xff) << 48) + ((long) (array[2] & 0xff) << 40) + ((long) (array[3] & 0xff) << 32)
				+ ((long) (array[4] & 0xff) << 24) + ((long) (array[5] & 0xff) << 16) + ((long) (array[6] & 0xff) << 8) + ((long) array[7] & 0xff);
	}

	@Override
	public void setValue(byte[] array, Long val) {

		array[0] = (byte) ((val >>> 56) & 0xff);
		array[1] = (byte) ((val >>> 48) & 0xff);
		array[2] = (byte) ((val >>> 40) & 0xff);
		array[3] = (byte) ((val >>> 32) & 0xff);
		array[4] = (byte) ((val >>> 24) & 0xff);
		array[5] = (byte) ((val >>> 16) & 0xff);
		array[6] = (byte) ((val >>> 8) & 0xff);
		array[7] = (byte) (val & 0xff);
	}

}
