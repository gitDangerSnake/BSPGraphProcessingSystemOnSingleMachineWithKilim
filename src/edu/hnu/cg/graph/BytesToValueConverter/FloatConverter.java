package edu.hnu.cg.graph.BytesToValueConverter;

public class FloatConverter implements BytesToValueConverter<Float> {

	@Override
	public int sizeOf() {
		return 4;
	}

	@Override
	public Float getValue(byte[] array) {
		int x = ((array[0] & 0xff) << 24) + ((array[1] & 0xff) << 16) + ((array[2] & 0xff) << 8) + (array[3] & 0xff);
		return Float.intBitsToFloat(x);
	}

	@Override
	public void setValue(byte[] array, Float val) {
		int x = Float.floatToIntBits(val);
		array[0] = (byte) ((x >>> 24) & 0xff);
		array[1] = (byte) ((x >>> 16) & 0xff);
		array[2] = (byte) ((x >>> 8) & 0xff);
		array[3] = (byte) ((x >>> 0) & 0xff);

	}

	
}
