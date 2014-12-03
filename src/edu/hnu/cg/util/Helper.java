package edu.hnu.cg.util;

import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

import edu.hnu.cg.graph.config.Configure;
import sun.misc.Unsafe;

public class Helper {

	public static Unsafe getUnsafe() {
		return THE_UNSAFE;
	}

	private static Unsafe THE_UNSAFE;
	static {
		try {
			@SuppressWarnings("unused")
			final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
				@Override
				public Unsafe run() throws Exception {
					Field theUnsafe = Unsafe.class
							.getDeclaredField("theUnsafe");
					theUnsafe.setAccessible(true);
					return (Unsafe) theUnsafe.get(null);
				}
			};

		} catch (Exception e) {
			throw new RuntimeException("Unable to load unsafe", e);
		}
	}
	
	
	public static long reverseOffset(long offset) {
		return (~offset) + 1;
	}

	public static long pack(int a, int b) {
		return ((long) a << 32) + b;
	}

	public static int getFirst(long e) {
		return (int) (e >> 32);
	}

	public static int getSecond(long e) {
		return (int) (e & 0x00000000ffffffffL);
	}

	

	private static Random random = new Random();

	private static int partition(long[] arr, byte[] values, int sizeof,
			int left, int right) {
		int i = left, j = right;
		long tmp;
		long pivot = arr[left + random.nextInt(right - left + 1)];
		byte[] valueTemplate = new byte[sizeof];

		while (i <= j) {
			while (arr[i] < pivot)
				i++;
			while (arr[j] > pivot)
				j--;

			if (i <= j) {
				// 交换edge
				tmp = arr[i];
				arr[i] = arr[j];
				arr[j] = tmp;
				// 同时交换发生交换的边所对应的value
				if (values != null) {
					System.arraycopy(values, j * sizeof, valueTemplate, 0,
							sizeof);
					System.arraycopy(values, i * sizeof, values, j * sizeof,
							sizeof);
					System.arraycopy(valueTemplate, 0, values, i * sizeof,
							sizeof);
				}

				i++;
				j--;
			}

		}

		return i;
	}

	public static void quickSort(long arr[], byte[] values, int sizeof, int left,
			int right) {
		if (left < right) {
			int index = partition(arr, values, sizeof, left, right);
			if (left < index - 1) {
				quickSort(arr, values, sizeof, left, index - 1);
			}
			if (index < right) {
				quickSort(arr, values, sizeof, index, right);
			}
		}
	}

	public static int byteArrayToInt(byte[] array) {
		return ((array[0] & 0xff) << 24) + ((array[1] & 0xff) << 16)
				+ ((array[2] & 0xff) << 8) + (array[3] & 0xff);
	}

	public static byte[] intToByteArray(int val) {
		byte[] array = new byte[4];
		array[3] = (byte) ((val) & 0xff);
		array[2] = (byte) ((val >>> 8) & 0xff);
		array[1] = (byte) ((val >>> 16) & 0xff);
		array[0] = (byte) ((val >>> 24) & 0xff);
		return array;
	}

	public static byte[] longToByteArray(long val) {
		byte[] array = new byte[8];

		array[0] = (byte) ((val >>> 56) & 0xff);
		array[1] = (byte) ((val >>> 48) & 0xff);
		array[2] = (byte) ((val >>> 40) & 0xff);
		array[3] = (byte) ((val >>> 32) & 0xff);
		array[4] = (byte) ((val >>> 24) & 0xff);
		array[5] = (byte) ((val >>> 16) & 0xff);
		array[6] = (byte) ((val >>> 8) & 0xff);
		array[7] = (byte) (val & 0xff);

		return array;
	}

	public static long byteArrayToLong(byte[] array) {
		return ((long) (array[0] & 0xff) << 56)
				+ ((long) (array[1] & 0xff) << 48)
				+ ((long) (array[2] & 0xff) << 40)
				+ ((long) (array[3] & 0xff) << 32)
				+ ((long) (array[4] & 0xff) << 24)
				+ ((long) (array[5] & 0xff) << 16)
				+ ((long) (array[6] & 0xff) << 8) + ((long) array[7] & 0xff);
	}
	
	
	

}
