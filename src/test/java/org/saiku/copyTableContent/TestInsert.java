package org.saiku.copyTableContent;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ylutil.JdbcRela;

import com.jolbox.bonecp.BoneCPDataSource;

public class TestInsert {
	public static void main(String args[]) throws SQLException{
		BoneCPDataSource bcpdsTo = JdbcRela.getBoncpDataSource(JdbcRela
				.getOracleDriver(), JdbcRela.getOracleConnFormatUrlWithDesc(
				"123.125.195.21", "1521", "ec_test"), "oracle_yctx",
				"yctx123456");
		Connection conn = bcpdsTo.getConnection();
		PreparedStatement pstmt = conn.prepareStatement("insert into log_register (id,passport,register_from,issuccess,registerdate) values(?,?,?,?,?)");
		pstmt.setInt(1, 1);
		pstmt.setString(2, "yanglu");
		pstmt.setString(3, "0200010009887");
		pstmt.setInt(4, 1);
		pstmt.setDate(5, new Date(System.currentTimeMillis()));
		pstmt.executeUpdate();
	}
}
