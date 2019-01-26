package com.spring.mr.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	private String phone;
	private Long up_flow;
	private Long down_flow;
	private Long sum_flow;
	
	public FlowBean(){}
	
	public FlowBean(String phone, Long up_flow, Long down_flow) {
		this.phone = phone;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = down_flow + up_flow;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		phone = in.readUTF();
		up_flow = in.readLong();
		down_flow = in.readLong();
		sum_flow = in.readLong();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
	}
	@Override
	public String toString() {
		return  up_flow + "\t" + down_flow + "\t" + sum_flow;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Long getUp_flow() {
		return up_flow;
	}

	public void setUp_flow(Long up_flow) {
		this.up_flow = up_flow;
	}

	public Long getDown_flow() {
		return down_flow;
	}

	public void setDown_flow(Long down_flow) {
		this.down_flow = down_flow;
	}

	public Long getSum_flow() {
		return sum_flow;
	}

	public void setSum_flow(Long sum_flow) {
		this.sum_flow = sum_flow;
	}

	
	
}
