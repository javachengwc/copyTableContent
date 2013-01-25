package org.saiku.copyTableContent;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.saiku.copyTableContent.exception.CopyTableException;

import ylutil.JdbcRela;

/**
 * Context 形状
 * column1    column2    column3  ....   column n
 *   |          |          |
 *   |          |          |
 *   |          |          |
 *   |          |          |
 *   |          |          |
 * @author yanglu
 *
 */
public class SourceDataContext {
	private static SourceDataContext context = null;
	private static Object lock = new Object();
	public static volatile AtomicInteger listNum = new AtomicInteger(0);
	private List<ColumnInfoAgg<?>> columnsList = new ArrayList<ColumnInfoAgg<?>>();
	private SourceDataContext(){
		
	}
	
	public void initColumnsInfo(ResultSet rs){
		try {
			addColumnInfo(rs);
		} catch (CopyTableException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			JdbcRela.close(rs, null, null);
		}
	}
	
	private void addColumnInfo(ResultSet rs) throws CopyTableException, SQLException{
		if(rs == null){
			throw new CopyTableException("找不到表");
		}
		while(rs.next()){
			String columnName = rs.getString("COLUMN_NAME"); 
			String columnType = rs.getString("TYPE_NAME"); 
			if(columnType.contains("VARCHAR") || columnType.contains("CHAR")){				
				ColumnInfoAgg<?> columnInfoAgg = new ColumnInfoAgg<String>();
				columnInfoAgg.setColumnName(columnName);
				columnInfoAgg.setColumnType(columnType);
				columnsList.add(columnInfoAgg);
			}else if(columnType.contains("DATE") || columnType.contains("TIME")){
				ColumnInfoAgg<?> columnInfoAgg = new ColumnInfoAgg<Date>();
				columnInfoAgg.setColumnName(columnName);
				columnInfoAgg.setColumnType(columnType);
				columnsList.add(columnInfoAgg);
			}else if(columnType.contains("NUMBER") || columnType.contains("INTEGER")){
				ColumnInfoAgg<?> columnInfoAgg = new ColumnInfoAgg<Integer>();
				columnInfoAgg.setColumnName(columnName);
				columnInfoAgg.setColumnType(columnType);
				columnsList.add(columnInfoAgg);
			}
		}
	}
	
	/**
	 * the best way is addColumn first,and then insert into the queue of the columnInfoAgg
	 * @param columnInfoAgg
	 */
	public void addColumn(ColumnInfoAgg<?> columnInfoAgg){
		columnsList.add(columnInfoAgg);
	}
	
	/**
	 * 从源数据导出来的数据在这儿处理，加入Context中
	 * rs -> context -> 清空rs
	 * context -> toDB
	 * @param rs
	 */
	public void parseRecords(ResultSet rs){
		if(rs == null){
			return;
		}
		try {
			while(rs.next()){
				for(int i = 0; i < columnsList.size(); i++){
					ColumnInfoAgg<?> columnInfo = columnsList.get(i);
					String columnName = columnInfo.getColumnName();
//					String columnType = columnInfo.getColumnType();
					if(columnInfo.getType() == ColumnInfoAgg.STRING_TYPE){
						@SuppressWarnings("unchecked")
						Queue<String> tmpQueue = (Queue<String>)columnInfo.getColumnQueue();
						String data = rs.getString(columnName);
						if(data == null){
							tmpQueue.add("");
						}else{							
							tmpQueue.add(data);
						}
						continue;
					}else if(columnInfo.getType() == ColumnInfoAgg.DATE_TYPE){
						@SuppressWarnings("unchecked")
						Queue<Timestamp> tmpQueue = (Queue<Timestamp>)columnInfo.getColumnQueue();
						Timestamp data = rs.getTimestamp(columnName);
						if(data == null){
							tmpQueue.add(new Timestamp(Date.valueOf("19710101").getTime()));
						}else{							
							tmpQueue.add(data);
						}
						continue;
					}else if(columnInfo.getType() == ColumnInfoAgg.NUMBER_TYPE){
						@SuppressWarnings("unchecked")
						Queue<Integer> tmpQueue = (Queue<Integer>)columnInfo.getColumnQueue();
						int data = rs.getInt(columnName);
						if(data == 0){
							tmpQueue.add(999);
						}else{
							tmpQueue.add(data);
						}
						continue;
					}else{
						columnInfo.getColumnQueue().add(null);
					}
				}
				SourceDataContext.listNum.incrementAndGet();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
//		finally{
//			JdbcRela.close(rs, null, null);
//		}
		System.out.println("parse over");
	}
	
	public List<ColumnInfoAgg<?>> getColumnList(){
		return columnsList;
	}
	
	
	public static SourceDataContext getInstance(){
		if(context == null){
			synchronized(lock){
				if(context == null){
					return new SourceDataContext();
				}
			}
		}
		return context;
	}
	
	public void clear(){
		for(int i = 0; i < columnsList.size(); i++){
			columnsList.get(i).getColumnQueue().clear();
		}
		columnsList.clear();
		columnsList = null;
		context = null;
	}
}
