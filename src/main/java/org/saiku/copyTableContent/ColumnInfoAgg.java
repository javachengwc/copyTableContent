package org.saiku.copyTableContent;

import java.sql.Date; 
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ColumnInfoAgg<E> {
	//
	public static final int BLOCKING_SIZE = 1000;
	
	private String columnName;
	private String columnType;
	
	//当放入insert语句中时所处的位置
	private int preparedPos = 0;
	
	private Queue<E> columnQueue = new ArrayBlockingQueue<E>(BLOCKING_SIZE + BLOCKING_SIZE / 2);
//	private Queue<E> columnQueue = null;
	
	//columnType
	public static final int NULL_TYPE = 0;
	public static final int STRING_TYPE = 1;
	public static final int DATE_TYPE = 2;
	public static final int NUMBER_TYPE = 3;
	
	public E pop(){
		return columnQueue.poll();
	}
	
	//加入null值的情况只能在外面进行处理了
//	public void add(E e){
//		columnQueue.add(e);
//	}
	
	public int getType(){
		String type = columnType.toUpperCase();
		if(type.contains("VARCHAR") || type.contains("CHAR")){
			return ColumnInfoAgg.STRING_TYPE;
		}else if(type.contains("DATE") || type.contains("TIME")){
			return ColumnInfoAgg.DATE_TYPE;
		}else if(type.contains("NUMBER") || type.contains("INTEGER")){
			return ColumnInfoAgg.NUMBER_TYPE;
		}else{
			return ColumnInfoAgg.NULL_TYPE;
		}
	}
	
	public void setValueForStatement(PreparedStatement stmt,E e) throws SQLException{
		int type = getType();
		switch(type){
		case ColumnInfoAgg.STRING_TYPE:
			stmt.setString(preparedPos, (String)e);
			break;
		case ColumnInfoAgg.DATE_TYPE:
			stmt.setDate(preparedPos, (Date)e);
			break;
		case ColumnInfoAgg.NUMBER_TYPE:
			stmt.setInt(preparedPos, (Integer)e);
			break;
		}
	}
	

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public Queue<E> getColumnQueue() {
		return columnQueue;
	}

	public void setColumnQueue(Queue<E> columnQueue) {
		this.columnQueue = columnQueue;
	}

	public static int getBlockingSize() {
		return BLOCKING_SIZE;
	}

	public int getPreparedPos() {
		return preparedPos;
	}

	public void setPreparedPos(int preparedPos) {
		this.preparedPos = preparedPos;
	}
	
	
}
