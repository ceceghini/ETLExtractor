package it.arg.etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SourceConnection {

	private static Connection conn;
	
	/**
	 * Restituisce la connessione al DB2 su as400
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws Exception {
		
		if (conn==null) {
			
			// Recupero la connessione del ambiente di origine
			int idSourceConnection = Integer.parseInt(OraConnection.getStepParm("SRC_CONNECTION"));
			
			Statement _stmt = OraConnection.getConnection().createStatement();	// Statement di recupero dei dati
			String sql = "select DRIVER, CS, USERNAME, PASSWORD from TB_SRC_CONNECTION where id_connection = " + idSourceConnection;
			ResultSet rs = _stmt.executeQuery(sql);
			if (rs.next()) {
				
				OraConnection.InfoLog("Connection.getConnection()", "Informazioni connessione sorgente recuperate.");
				
				//DriverManager.registerDriver (new com.ibm.as400.access.AS400JDBCDriver());
				Class.forName(rs.getString(1));
				conn = DriverManager.getConnection(rs.getString(2), rs.getString(3), rs.getString(4));
				
				OraConnection.InfoLog("Connection.getConnection()", "Connessione sorgente istanziata.");
				
			}
			else {
				OraConnection.LogAppException("Connection.getConnection()", "Informazioni di connessione non recuperate: ["+idSourceConnection+"]");
			}
			rs.close();
		}
		return conn;
		
	}
	
	public static Connection getConnection(String driver, String cs, String username, String password) throws Exception {
		
		if (conn==null) {
			Class.forName(driver);
			conn = DriverManager.getConnection(cs, username, password);
		}
		return conn;
		
	}
	
}
