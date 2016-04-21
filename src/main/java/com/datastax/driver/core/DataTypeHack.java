package com.datastax.driver.core;

import io.netty.buffer.ByteBuf;

public class DataTypeHack {
    static DataType decode(ByteBuf buffer, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    	return DataType.decode(buffer, protocolVersion, codecRegistry);
    }

}
