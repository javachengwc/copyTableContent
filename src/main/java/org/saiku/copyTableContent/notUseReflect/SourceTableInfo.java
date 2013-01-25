package org.saiku.copyTableContent.notUseReflect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.saiku.copyTableContent.ColumnInfoAgg;
import org.saiku.copyTableContent.SourceDataContext;
import org.springframework.jdbc.core.JdbcTemplate;

import ylutil.JdbcRela;

public class SourceTableInfo {
	private DataSource dataSource;
	private String tableName;
	private JdbcTemplate jdbcTemplate;

	public SourceTableInfo(DataSource dataSource, String tableName) {
		this.dataSource = dataSource;
		this.tableName = tableName;
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public int getMaxId() {
		return jdbcTemplate.queryForInt("select max(id) from " + tableName);
	}

	public int getMinId() {
		return jdbcTemplate.queryForInt("select min(id) from " + tableName);
	}

	public int getSize() {
		return jdbcTemplate.queryForInt("select max(id) from " + tableName);
	}

	/**
	 * 
	 * @return
	 */
	public int getTimes() {
		int maxId = getMaxId();
		int minId = getMinId();
		System.out.println("maxId = " + maxId + "  minId = " + minId);
		return (maxId - minId) % ColumnInfoAgg.BLOCKING_SIZE == 0 ? (maxId - minId)
				/ ColumnInfoAgg.BLOCKING_SIZE
				: (maxId - minId) / ColumnInfoAgg.BLOCKING_SIZE + 1;
	}

	/**
	 * beacause the Bean has not defined ,so it is not a good way to use
	 * jdbcTemplate here
	 * 
	 * @param fromId
	 * @param toId
	 * @return
	 * @throws SQLException
	 */
	public ResultSet readRecords(int fromId, int toId) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement("select * from "
					+ tableName + " where id >= " + fromId + " and id < " + toId);
			rs = pstmt.executeQuery();
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}finally{
			JdbcRela.close(null, pstmt, conn);
		}
	}
	
	public void parseRecords(SourceDataContext context,int fromId, int toId){
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement("select * from "
					+ tableName + " where id >= " + fromId + " and id < " + toId);
			rs = pstmt.executeQuery();
			context.parseRecords(rs);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			JdbcRela.close(rs, pstmt, conn);
		}
	}
	
	public ResultSet getTableColumns(){
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			DatabaseMetaData dbmd = conn.getMetaData(); 
			ResultSet rs = dbmd.getColumns(null, "%", tableName.toUpperCase(), "%");
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}finally{
			JdbcRela.close(null, null, conn);
		}
	}

}
