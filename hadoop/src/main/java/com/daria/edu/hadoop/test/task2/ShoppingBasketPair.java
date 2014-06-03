package com.daria.edu.hadoop.test.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * Writable element which has information about pair values.
 * @author Daria_Serkova 
 */
public class ShoppingBasketPair implements
		WritableComparable<ShoppingBasketPair> {

	private int firstId;  //less value
	private int secondId; //bigger value

	public ShoppingBasketPair() {}

	public ShoppingBasketPair(int firstId, int secondId) {
		if (firstId < secondId) {
			this.firstId = firstId;
			this.secondId = secondId;
		} else {
			this.firstId = secondId;
			this.secondId = firstId;
		}
	}

	public int getFirstId() {
		return firstId;
	}

	public int getSecondId() {
		return secondId;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(firstId);
		out.writeInt(secondId);

	}

	public void readFields(DataInput in) throws IOException {
		firstId = in.readInt();
		secondId = in.readInt();
	}

	public int compareTo(ShoppingBasketPair o) {
		int otherFirst = o.getFirstId();
		int otherSecond = o.getSecondId();
		if ((firstId == otherFirst || firstId == otherSecond)
				&& (secondId == otherFirst || secondId == otherSecond)) {
			return 0;
		}
		if (firstId == otherFirst){
			return Integer.compare(secondId, otherSecond);
		}
		return Integer.compare(firstId, otherFirst);
	}

	@Override
	public String toString() {
		return "[" + firstId + "; " + secondId + "]";
	}

	@Override
	public int hashCode() {
		int hash = 31;
		hash = hash * firstId;
		hash = hash * secondId;
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		ShoppingBasketPair other = (ShoppingBasketPair) obj;
		if ((firstId == other.getFirstId() || firstId == other.getSecondId())
				&& (secondId == other.getFirstId() || secondId == other
						.getSecondId()))
			return true;
		return false;
	}
}