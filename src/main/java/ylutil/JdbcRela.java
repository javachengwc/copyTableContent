package ylutil;

import java.sql.Connection; 
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.jolbox.bonecp.BoneCPDataSource;

/**
 * 1 forClass
 * 2 DriverManager.getConnection("描述信息")
 * @author yanglu
 *
 */
public class JdbcRela {
	public static void registerMySqlDriver() throws ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
	}

	public static void registerOracleDriver() throws ClassNotFoundException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
	}
	
	public static String getMySqlDriver(){
		return "com.mysql.jdbc.Driver";
	}
	
	public static String getOracleDriver(){
		return "oracle.jdbc.driver.OracleDriver";
	}

	/**
	 * address = ip+host
	 * 
	 * @param address
	 * @return
	 */
	public static String getMySqlConnFormatUrl(String address, String dbName) {
		return "jdbc:mysql://" + address + "/" + dbName
				+ "?characterEncoding=UTF-8";
	}

	/**
	 * init boncpDataSource
	 * @param boneDs
	 */
	public static BoneCPDataSource getBoncpDataSource(String driver, String url, String userName, String password) {
		BoneCPDataSource boneDs = new com.jolbox.bonecp.BoneCPDataSource();
		boneDs.setDriverClass(driver);
		boneDs.setJdbcUrl(url);
		boneDs.setUsername(userName);
		boneDs.setPassword(password);
		boneDs.setIdleConnectionTestPeriod(60);
		boneDs.setIdleConnectionTestPeriodInMinutes(50);
		boneDs.setIdleMaxAge(240);
		boneDs.setMaxConnectionsPerPartition(30);
		boneDs.setMinConnectionsPerPartition(10);
		boneDs.setPartitionCount(3);
		boneDs.setAcquireIncrement(5);
		boneDs.setStatementCacheSize(100);
		boneDs.setReleaseHelperThreads(3);
		return boneDs;
	}

	/**
	 * 有描述信息的oracle
	 * 
	 * @param address
	 * @param dbName
	 * @return
	 */
	public static String getOracleConnFormatUrlWithDesc(String ip, String host,
			String dbName) {
		return "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)"
				+ "(HOST = "
				+ ip
				+ ")(PORT = "
				+ host
				+ ")))"
				+ "(CONNECT_DATA =(SERVER = DEDICATED)"
				+ "(SERVICE_NAME = "
				+ dbName + ")))";
	}

	public static String getOracleConnUrl(String address, String ecWorld) {
		return "jdbc:oracle:thin:@//" + address + "/" + ecWorld;
	}

	/**
	 * close re,stmt,conn
	 * 
	 * @param rs
	 * @param stmt
	 * @param conn
	 */
	public static void close(ResultSet rs, Statement stmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
