package edu.hnu.bgpsa.graph.framework;

import edu.hnu.cg.graph.BytesToValueConverter.BytesToValueConverter;
import edu.hnu.cg.graph.BytesToValueConverter.IntConverter;
import edu.hnu.cg.graph.BytesToValueConverter.MsgBytesTovalueConverter;

public class MsgConverter<MsgValueType> implements MsgBytesTovalueConverter<MsgValueType> {

	private BytesToValueConverter<MsgValueType> msgValueBytesToValueConverter;

	public MsgConverter(BytesToValueConverter<MsgValueType> c) {
		msgValueBytesToValueConverter = c;
	}

	@Override public int getFrom(byte[] msg) {
		byte[] id = new byte[4];
		System.arraycopy(msg, 0, id, 0, 4);
		return new IntConverter().getValue(id);
	}

	@Override public void setFrom(int from, byte[] msg) {
		IntConverter ic = new IntConverter();
		byte[] idTemplate = new byte[4];
		ic.setValue(idTemplate, from);
		System.arraycopy(idTemplate, 0, msg, 0, 4);
	}

	@Override public int getTo(byte[] msg) {
		byte[] id = new byte[4];
		System.arraycopy(msg, 4, id, 0, 4);
		return new IntConverter().getValue(id);
	}

	@Override public void setTo(int to, byte[] msg) {
		IntConverter ic = new IntConverter();
		byte[] idTemplate = new byte[4];
		ic.setValue(idTemplate, to);
		System.arraycopy(idTemplate, 0, msg, 4, 4);
	}

	@Override
	public int sizeOf() {
		return 8 + msgValueBytesToValueConverter.sizeOf();
	}

	@Override
	public MsgValueType getValue(byte[] msg) {
		byte[] value = new byte[msgValueBytesToValueConverter.sizeOf()];
		System.arraycopy(msg, 8, value, 0, msgValueBytesToValueConverter.sizeOf());
		return msgValueBytesToValueConverter.getValue(value);
	}

	@Override
	public void setValue(byte[] msg, MsgValueType val) {
		byte[] value = new byte[msgValueBytesToValueConverter.sizeOf()];
		msgValueBytesToValueConverter.setValue(value, val);
		System.arraycopy(value, 0, msg, 8, value.length);
	}

}
