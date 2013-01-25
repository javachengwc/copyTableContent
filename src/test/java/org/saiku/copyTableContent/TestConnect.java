package org.saiku.copyTableContent;


public class TestConnect {
	public static void main(String args[]) throws ClassNotFoundException{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		String url = "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)"
				+ "(HOST = "
				+ "192.168.1.24"
				+ ")(PORT = "
				+ "1521"
				+ ")))"
				+ "(CONNECT_DATA =(SERVER = DEDICATED)"
				+ "(SERVICE_NAME = "
				+ "ecworld" + ")))";
	}
}
