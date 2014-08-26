package it.arg.etl;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class MetaData {

	private HashMap _colType;
	private int _numOfArgument = 0;
	
	private int _InsertType;
	
	private final int INSERT_BY_PROC = 1;
	//private final int INSERT_BY_SQLTAG = 2;
	private final int INSERT_BY_SQLPOS = 3;
	
	private CallableStatement _cStmt;
	private PreparedStatement _pStmt;
	private String statement;
	
	public MetaData(String insert_statement) throws Exception {
		
		// Verifica la tipologia di statement di insert
		if (insert_statement.substring(1, 6).compareTo("{ call")==0) {
			// Si tratta di un inserimento fatto tramite procedura pl/sql
			this._InsertType = INSERT_BY_PROC;
		}
		else {
			this._InsertType = INSERT_BY_SQLPOS;
		}
		
		_colType = new HashMap();
		
		// Decodifica dei parametri
		if (_InsertType==INSERT_BY_PROC) {
			
			OraConnection.InfoLog("MetaData", "Inserimento di tipo ByProc. Si procede con la generazione dello statement.");
			
			Statement s = OraConnection.getConnection().createStatement();
			insert_statement = removeChar(insert_statement, '{');
			insert_statement = removeChar(insert_statement, '}').trim();
			ResultSet rs = s.executeQuery("select POSITION, DATA_TYPE from user_arguments where package_name || '.' || object_name = '" + insert_statement + "' order by position");
			
			statement = "{ call " + insert_statement + " (";
			
			while (rs.next()) {
				_colType.put(rs.getString(1), rs.getString(2));
				statement += "?, ";
				_numOfArgument ++;
			}
			rs.close();
			
			statement = statement.substring(0, statement.length()-2);
			statement += ") }";
			this._cStmt = OraConnection.getConnection().prepareCall(statement);
			
			OraConnection.InfoLog("MetaData", "Statement generato: " + statement);
			
		}
		
		//	Decodifica dei parametri
		if (_InsertType==INSERT_BY_SQLPOS) {
			
			OraConnection.InfoLog("MetaData", "Inserimento di tipo ByInsert. Si procede con la generazione dello statement.");
			
			Statement s = OraConnection.getConnection().createStatement();
			ResultSet rs = s.executeQuery("select column_id, data_type from all_tab_columns where owner || '.' || table_name = '"+ insert_statement +"' order by column_id");
			
			statement = "insert into " + insert_statement + " values (";
			
			while (rs.next()) {
				_colType.put(rs.getString(1), rs.getString(2));
				statement += "?, ";
				_numOfArgument ++;
			}
			rs.close();
			
			statement = statement.substring(0, statement.length()-2);
			statement += ")";
			this._pStmt = OraConnection.getConnection().prepareStatement(statement);
			
			OraConnection.InfoLog("MetaData", "Statement generato: " + statement);
			
		}
		
		
	}
	
	public void Call(ResultSet rs) throws Exception {
		if (_InsertType==INSERT_BY_PROC) {
			CallCallableStatement(rs);
		}
		if (_InsertType==INSERT_BY_SQLPOS) {
			CallPreparedStatement(rs);
		}
	}
	
	public void CallCallableStatement(ResultSet rs) throws Exception {
		
		String s;
		String colType;
//		 Loop fra tutte le colonne del cursore
		for (int i=1;i<=this._numOfArgument;i++) {
				
			s = String.valueOf(i);
			colType = this._colType.get(s).toString();
			
			if (colType.compareTo("VARCHAR2")==0) {
				_cStmt.setString(i, rs.getString(i));
			}
			else if (colType.compareTo("CHAR")==0) {
				_cStmt.setString(i, rs.getString(i));
			}
			else if (colType.compareTo("NUMBER")==0) {
				_cStmt.setBigDecimal(i, rs.getBigDecimal(i));
			}
			else if (colType.compareTo("DATE")==0) {
				_cStmt.setDate(i, rs.getDate(i));
			}
			else {
				OraConnection.LogAppException(this.getClass().toString()+".Execute", "Mapping della tipologia di dato della colonna non riuscito. COLUMN: ["+ i +"] - TYPE: ["+ colType +"]");
				return;
			}
			
		}
		
		_cStmt.execute();
		
	}
	
public void CallPreparedStatement(ResultSet rs) throws Exception {
		
		String s;
		String colType;
//		 Loop fra tutte le colonne del cursore
		for (int i=1;i<=this._numOfArgument;i++) {
				
			s = String.valueOf(i);
			colType = this._colType.get(s).toString();
			
			if (colType.compareTo("VARCHAR2")==0) {
				_pStmt.setString(i, rs.getString(i));
			}
			else if (colType.compareTo("CHAR")==0) {
				_pStmt.setString(i, rs.getString(i));
			}
			else if (colType.compareTo("NUMBER")==0) {
				_pStmt.setBigDecimal(i, rs.getBigDecimal(i));
			}
			else if (colType.compareTo("DATE")==0) {
				_pStmt.setDate(i, rs.getDate(i));
			}
			else {
				OraConnection.LogAppException(this.getClass().toString()+".Execute", "Mapping della tipologia di dato della colonna non riuscito. COLUMN: ["+ i +"] - TYPE: ["+ colType +"]");
				return;
			}
			
		}
		
		_pStmt.execute();
		
	}

	private String removeChar(String s, char c) {
	   String r = "";
	   for (int i = 0; i < s.length(); i ++) {
	      if (s.charAt(i) != c) r += s.charAt(i);
	   }
	   return r;
	}
}
