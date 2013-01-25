package org.saiku.copyTableContent.notUseReflect;

import java.sql.Connection;
//import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.saiku.copyTableContent.ColumnInfoAgg;
import org.saiku.copyTableContent.SourceDataContext;

import ylutil.JdbcRela;

public class ToTableDealer implements Runnable{
	private SourceDataContext context;
	private DataSource dataSource;
	private String toTable;
	private static ReentrantLock trlock = new ReentrantLock();
	
	//为了产生多个线程
	public static volatile AtomicInteger threadNum = new AtomicInteger(0);
	public static Object lock = new Object();

	public ToTableDealer(SourceDataContext context, DataSource dataSource,
			String toTable) {
		this.context = context;
		this.dataSource = dataSource;
		this.toTable = toTable;
	}

	public void run() {
		try {
			write2Table();
			ToTableDealer.threadNum.decrementAndGet();
			System.out.println("threadNum = " + ToTableDealer.threadNum.get());
			if(ToTableDealer.threadNum.get() <= 0){
				synchronized(lock){
					lock.notify();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 把方法定义为synchronized级别的原因是只有这一个地方是从queue中读取数据
	 * 不选择用JdbcTemplate原因在于我们取出来的数据装载不了成bean
	 * @throws SQLException 
	 */
	public void write2Table() throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			String sql = prepareSql();
			System.out.println(sql);
			pstmt = conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		List<ColumnInfoAgg<?>> columnsList = context.getColumnList();
		
		ToTableDealer.trlock.lock();
		while (SourceDataContext.listNum.get() > 0) {
//			System.out.println("listNum = " + SourceDataContext.listNum.get());
			for (int i = 0; i < columnsList.size(); i++) {
				int type = columnsList.get(i).getType();
				switch (type) {
				case ColumnInfoAgg.STRING_TYPE:
					String content = (String)columnsList.get(i).pop();
//					System.out.println(content);
					pstmt.setString(columnsList.get(i).getPreparedPos(), content);
					break;
				case ColumnInfoAgg.DATE_TYPE:
					Timestamp content1 = (Timestamp) columnsList.get(i).pop();
//					System.out.println(content1);
					pstmt.setTimestamp(columnsList.get(i).getPreparedPos(), content1);
					break;
				case ColumnInfoAgg.NUMBER_TYPE:
					int content3 = (Integer) columnsList.get(i).pop();
//					System.out.println(content3);
					pstmt.setInt(columnsList.get(i).getPreparedPos(), content3);
					break;
				}
			}
			pstmt.addBatch();
			SourceDataContext.listNum.decrementAndGet();
		}
		ToTableDealer.trlock.unlock();
		System.out.println("start update....");
		pstmt.executeBatch();
		conn.commit();
		
		System.out.println("update over...");
		JdbcRela.close(null, pstmt, conn);
	}

	private String prepareSql() {
		String sql = "insert into " + toTable + " (";
		List<ColumnInfoAgg<?>> columnsList = context.getColumnList();
		for (int i = 0; i < columnsList.size(); i++) {
			String columnName = columnsList.get(i).getColumnName();
			sql += columnName + ",";
			columnsList.get(i).setPreparedPos(i + 1);
		}
		sql = sql.substring(0, sql.length() - 1) + ")";
		sql += " values(";
		for (int i = 0; i < columnsList.size(); i++) {
			sql += "?,";
		}
		sql = sql.substring(0, sql.length() - 1) + ")";
		return sql;
	}
	
	
	public void createTable(String tableName){
		//从Context处获得表结构信息
		//create table
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = dataSource.getConnection();
			String createTableSql = "create table " + tableName + "(";
			SourceDataContext context = SourceDataContext.getInstance();
			for(int i = 0; i < context.getColumnList().size(); i++){
				String columnType = context.getColumnList().get(i).getColumnType();
				String columnName = context.getColumnList().get(i).getColumnName();
				createTableSql += columnName + " " + columnType + ",";
			}
			createTableSql += ")";
			pstmt = conn.prepareStatement(createTableSql);
			pstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			JdbcRela.close(null, pstmt, conn);
		}
		
	}

}
