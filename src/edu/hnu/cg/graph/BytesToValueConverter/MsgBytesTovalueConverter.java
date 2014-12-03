package edu.hnu.cg.graph.BytesToValueConverter;

public interface MsgBytesTovalueConverter<MsgValueType> extends BytesToValueConverter<MsgValueType> {
	int getFrom(byte[] msg);
	void setFrom(int from,byte[] msg);
	int getTo(byte[] msg);
	void setTo(int to,byte[] msg);
}
