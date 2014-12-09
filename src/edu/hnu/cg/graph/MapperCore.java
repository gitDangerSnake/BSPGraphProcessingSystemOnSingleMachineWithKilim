package edu.hnu.cg.graph;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import edu.hnu.cg.graph.config.Configure;

public class MapperCore {

	private static String baseFile;

	

	private List<ByteBuffer> chunks = new ArrayList<>();
	private final static long TWOGIG = Integer.MAX_VALUE;
	private long length;
	private File coreFile;
	private RandomAccessFile coreFileAccessor;
	


	public MapperCore(String filename) {
		coreFile = new File(filename);
		// This is a for testing - to avoid the disk filling up
//		coreFile.deleteOnExit();
		// Now create the actual file
		try {
			coreFileAccessor = new RandomAccessFile(coreFile, "rw");

			FileChannel channelMapper = coreFileAccessor.getChannel();
			long size = 0;
			size = coreFileAccessor.length();
			long nChunks = size / TWOGIG;
			if (nChunks > Integer.MAX_VALUE)
				throw new ArithmeticException("Requested File Size Too Large");
			length = size;
			long countDown = size;
			long from = 0;
			while (countDown > 0) {
				long len = Math.min(TWOGIG, countDown);
				ByteBuffer chunk = channelMapper.map(MapMode.READ_WRITE, from,
						len);
				chunks.add(chunk);
				from += len;
				countDown -= len;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getLength(int offset) {
		return 0;
	}

	public byte[] get(long offSet, int size) throws IOException {
		// Quick and dirty but will go wrong for massive numbers
		double a = offSet;
		double b = TWOGIG;
		byte[] dst = new byte[size];
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offSet - whichChunk * TWOGIG;
		// Data does not straddle two chunks
		try {
			if (TWOGIG - withinChunk > dst.length) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position((int) withinChunk);
				readBuffer.get(dst, 0, dst.length);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				int l2 = (int) dst.length - l1;
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position((int) withinChunk);
				readBuffer.get(dst, 0, l1);

				chunk = chunks.get((int) whichChunk + 1);
				readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position(0);
				try {
					readBuffer.get(dst, l1, l2);
				} catch (java.nio.BufferUnderflowException e) {
					throw e;
				}
			}
		} catch (IndexOutOfBoundsException i) {
			i.printStackTrace() ;
		}
		return dst;
	}

	public void put(long offSet, byte[] src) throws IOException {
		// Quick and dirty but will go wrong for massive numbers
		double a = offSet;
		double b = TWOGIG;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offSet - whichChunk * TWOGIG;
		// Data does not straddle two chunks
		try {
			if (TWOGIG - withinChunk > src.length) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer writeBuffer = chunk.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(src, 0, src.length);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				int l2 = (int) src.length - l1;
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer writeBuffer = chunk.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(src, 0, l1);

				chunk = chunks.get((int) whichChunk + 1);
				writeBuffer = chunk.duplicate();
				writeBuffer.position(0);
				writeBuffer.put(src, l1, l2);

			}
		} catch (IndexOutOfBoundsException i) {
			throw new IOException("Out of bounds");
		}
	}

	public void purge() {
		if (coreFileAccessor != null) {
			try {
				coreFileAccessor.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				coreFile.delete();
			}
		}
	}

	public long getSize() {
		return length;
	}

	@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
	private void clean(final MappedByteBuffer buffer) {
		AccessController.doPrivileged(new PrivilegedAction() {
			@Override
			public Object run() {
				try {
					Method getCleanerMethod = buffer.getClass().getMethod(
							"cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod
							.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}
}