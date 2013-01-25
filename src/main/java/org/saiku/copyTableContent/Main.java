package org.saiku.copyTableContent;

import javax.sql.DataSource;

import org.saiku.copyTableContent.notUseReflect.SourceTableInfo;
import org.saiku.copyTableContent.notUseReflect.ToTableDealer;

import ylutil.JdbcRela;

public class Main {
	public static void main(String args[]) {
		String tables[] = {
				
				
		};
		for (String logTable : tables) {
			String fromTable = logTable;
			String toTable = logTable;
			
			/**
			 * -----------------------------------------------------------------------
			 * -----------------------------------------------------------------------
			 */
			SourceDataContext context = SourceDataContext.getInstance();

			// 读取源数据
			// 数据来源
			String fromDriver = JdbcRela.getOracleDriver();
			String fromUrl = JdbcRela.getOracleConnFormatUrlWithDesc(
					"132.12.2.12", "1521", "hello");
			DataSource fromDataSource = JdbcRela.getBoncpDataSource(fromDriver,
					fromUrl, "user", "password");

			// 写数据源
			String toDriver = JdbcRela.getOracleDriver();
			String toUrl = JdbcRela.getOracleConnFormatUrlWithDesc(
			// "123.125.195.21", "1521", "ec_test");
					"192.168.1.24", "1521", "ecwdsdfsdsdorsdfdld");
			DataSource toDataSource = JdbcRela.getBoncpDataSource(toDriver,
					toUrl, "sdfsdf", "sdfsdfdsf");

			// 源数据信息
			SourceTableInfo sourceTableInfo = new SourceTableInfo(
					fromDataSource, fromTable);
			//
			System.out.println("init context start...");
			context.initColumnsInfo(sourceTableInfo.getTableColumns());
			System.out.println("init context end....");
			int readTimes = sourceTableInfo.getTimes();
			int minId = sourceTableInfo.getMinId();

			//写线程的数量
			final int WRITE_THREAD_NUM = 5;

			for (int i = 0; i < readTimes; i++) {
				ToTableDealer.threadNum.set(WRITE_THREAD_NUM);
				// 写入缓存
				int fromId = minId + i * ColumnInfoAgg.BLOCKING_SIZE;
				int toId = minId + (i + 1) * ColumnInfoAgg.BLOCKING_SIZE;

				// ResultSet rs = sourceTableInfo.readRecords(fromId, toId);
				// context.parseRecords(rs);

				sourceTableInfo.parseRecords(context, fromId, toId);

				// 从缓存读取
				for (int ti = 0; ti < ToTableDealer.threadNum.get(); ti++) {
					System.out.println("write begin");
					ToTableDealer toTableDealer = new ToTableDealer(context,
							toDataSource, toTable);
					Thread t = new Thread(toTableDealer);
					t.start();
					// try {
					// toTableDealer.write2Table();
					// } catch (SQLException e) {
					// e.printStackTrace();
					// }
				}
				synchronized (ToTableDealer.lock) {
					try {
						System.out.println("wait");
						ToTableDealer.lock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			//clear
			SourceDataContext.getInstance().clear();
		}
	}
}
