package edu.hnu.cg.graph.BytesToValueConverter;


public class DoubleConverter implements BytesToValueConverter<Double> {

	@Override
	public int sizeOf() {
		return 8;
	}

	@Override
	public Double getValue(byte[] array) {
		long x = ((long) (array[0] & 0xff) << 56) + ((long) (array[1] & 0xff) << 48) + ((long) (array[2] & 0xff) << 40) + ((long) (array[3] & 0xff) << 32)
				+ ((long) (array[4] & 0xff) << 24) + ((long) (array[5] & 0xff) << 16) + ((long) (array[6] & 0xff) << 8) + ((long) array[7] & 0xff);
		return Double.longBitsToDouble(x);
	}

	@Override
	public void setValue(byte[] array, Double val) {

		long x = Double.doubleToLongBits(val);

		array[0] = (byte) ((x >>> 56) & 0xff);
		array[1] = (byte) ((x >>> 48) & 0xff);
		array[2] = (byte) ((x >>> 40) & 0xff);
		array[3] = (byte) ((x >>> 32) & 0xff);
		array[4] = (byte) ((x >>> 24) & 0xff);
		array[5] = (byte) ((x >>> 16) & 0xff);
		array[6] = (byte) ((x >>> 8) & 0xff);
		array[7] = (byte) (x & 0xff);

	}
	
	public static void main(String[] args) {
		DoubleConverter dc = new DoubleConverter();
		byte[] arr = new byte[8];
		dc.setValue(arr, 3.14);
		System.out.println(dc.getValue(arr));
		
		
	}

}
