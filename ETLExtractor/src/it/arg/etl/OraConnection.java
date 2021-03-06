package it.arg.etl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OraConnection {

	private static Connection conn;
	private static CallableStatement config_stmt;
	
	private static CallableStatement procLogAppException;
	private static CallableStatement procLogInfo;
	
	private static boolean _withOracleLog = false;
	private static boolean _infoLog = false;
	//private static boolean _logToFile = false;
	
	private static final int OracleTypeVarchar = 12;
	
	private static int _idStep; 
	
	/**
	 * Restituisce una connessione al db oracle corrente
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection(int idStep) throws SQLException {
		
		if (conn==null) {
			conn = DriverManager.getConnection("jdbc:default:connection:");
			_withOracleLog = true;
			_infoLog = true;
			intiWtlLogger();
		}
		
		_idStep = idStep;
		
		return conn;
		
	}
	
	public static Connection getConnection() {
		
		return conn;
		
	}
	
	public static Connection getConnection(int idStep, String cs, String userName, String password, boolean infolog, boolean commandLine) throws Exception {
		
		if (conn==null) {
			//DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@"+cs, userName, password);
			_idStep = idStep;
			
			// Imposto il log level to info nel caso in cui sia chiamata da commandline
			_infoLog = infolog;
			//if (commandLine)
			//	_infoLog = infolog;
			//else {
			if (!commandLine) {
				// Inizializza il logger sullo schema etl
				CallableStatement proc = conn.prepareCall("{ call wtl_logger.resumejournal }");
				proc.execute();
				_withOracleLog = true;
				intiWtlLogger();
			}
			
		}
		return conn;
		
	}
	
	/**
	 * Inizializzazione delle procedure di logging
	 */
	private static void intiWtlLogger() throws SQLException {
		
		procLogAppException = conn.prepareCall("{ call wtl_logger.logappexception(?, ?, ?) }");
		
		procLogInfo = conn.prepareCall("{ call wtl_logger.infolog(?, ?) }");
		
	}
	
	/**
	 * Recupero di un parametro dalla tabella tb_config nello schema suetl01
	 * @param param
	 * @return
	 * @throws SQLException
	 */
	public static String getConfig(String param) throws SQLException {
		
		InfoLog("getConfig", "Recupero parametro [" + param + "]");
		
		// Verifica l'esistenza dello statement
		if (config_stmt == null) {
			config_stmt = conn.prepareCall("{ ? = call suetl01.wtlpk_util.getparm(?)}");
			config_stmt.registerOutParameter(1, OracleTypeVarchar);
		}
		
		// Call dello statement
		config_stmt.setString(2, param);
		config_stmt.execute();
		
		InfoLog("getConfig", "Parametro [" + param + "] recuperato: ["+config_stmt.getString(1)+"]");
		
		return config_stmt.getString(1);
		
	}
	
	/**
	 * Recupero di un parametro dalla tabella tb_config nello schema suetl01
	 * @param param
	 * @return
	 * @throws SQLException
	 */
	public static String getStepParm(String param) throws SQLException {
		
		InfoLog("getConfig", "Recupero parametro [" + param + "]");
		
		// Verifica l'esistenza dello statement
		if (config_stmt == null) {
			config_stmt = conn.prepareCall("{ ? = call wtlpk_util.getstepparm(?, ?)}");
			config_stmt.registerOutParameter(1, OracleTypeVarchar);
		}
		
		// Call dello statement
		config_stmt.setInt(2, _idStep);
		config_stmt.setString(3, param);
		config_stmt.execute();
		
		InfoLog("getStepParm", "Step [" + _idStep + "] Parametro [" + param + "] recuperato: ["+config_stmt.getString(1)+"]");
		
		return config_stmt.getString(1);
		
	}
	
	/**
	 * Log dell'exception nel motore etl
	 * @param message		Messaggio dell'eccezzione
	 * @throws Exception
	 */
	public static void LogAppException(String contesto, String message) {
		LogAppException(-20009, contesto, message);
	}
	
	/**
	 * Log dell'exception nel motore etl
	 * @param code			Codice dell'eccezzione
	 * @param message		Messaggio dell'eccezzione
	 * @throws Exception
	 */
	public static void LogAppException(int code, String contesto, String message) {
		
		if (_withOracleLog) {
			try {
				System.out.println(contesto + " - " + message);
				procLogAppException.setInt(1, code);
				procLogAppException.setString(2, "DB2Extractor."+contesto);
				procLogAppException.setString(3, message);
				procLogAppException.execute();
			}
			catch (SQLException e) {

			}
		}
		else {
			System.out.println(contesto + " - " + message);
		}
	}
	
	/**
	 * Log di un informazione nel motore etl
	 * @param message		Messaggio dell'eccezzione
	 * @throws Exception
	 */
	public static void InfoLog(String contesto, String message) {
		
		if (!_infoLog)
			return;
		
		if (_withOracleLog) {
			try {
				//System.out.println(contesto + " - " + message);
				procLogInfo.setString(1, "DB2Extractor."+contesto);
				procLogInfo.setString(2, message);
				procLogInfo.execute();
			}
			catch (SQLException e) {
				
			}
		}
		else {
			if (_infoLog)
				System.out.println(contesto + " - " + message);
		}
	}
	
}