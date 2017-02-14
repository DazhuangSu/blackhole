package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class RollRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String partitionId;
    public long rollPeriod;
    public long currentPeriod;
    
    public RollRequest() {
    }
    
    public RollRequest(String topic, String partitionId, long rollPeriod, long currentPeriod) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.rollPeriod = rollPeriod;
        this.currentPeriod = currentPeriod;
    }
    
    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partitionId) + (Long.SIZE * 2) / 8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        partitionId = GenUtil.readString(buffer);
        rollPeriod = buffer.getLong();
        currentPeriod = buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partitionId, buffer);
        buffer.putLong(rollPeriod);
        buffer.putLong(currentPeriod);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.RotateOrRollRequest;
    }
}
