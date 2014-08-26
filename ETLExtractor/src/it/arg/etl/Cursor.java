package it.arg.etl;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Cursor {

	private String _sql;
	private String _truncStatement;
	
	private Connection _mssqlc;
	private PreparedStatement _stmt;
		
	private MetaData _meta;
	
	public Cursor(int idStep) throws Exception {
		//	Recupero le connessioni al db oracle e al db db2
		OraConnection.getConnection(idStep);
		initComponent();
	}
	
	public Cursor() throws Exception {
		
		// Recupero le connessioni al db oracle e al db db2
		initComponent();
		
	}
	
	private void initComponent() throws Exception {
			
		// Recupero della select sul sistema sorgente e della procedura di inserimento
		this._sql = OraConnection.getStepParm("SRC_SQL");
		this._truncStatement = OraConnection.getStepParm("EXEC_BEFORE");
		
		// Recupero i metadata della procedura di inserimento
		_meta = new MetaData(OraConnection.getStepParm("DEST_PROC"));
		
		// Statement sul sistema sorgente
		_mssqlc = SourceConnection.getConnection();
		
		OraConnection.InfoLog("Cursor.initComponent()", "Recupero stantement sql sistema sorgente.");
		_stmt = _mssqlc.prepareStatement(_sql);	// Statement di recupero dei dati
		
	}
	
	/**
	 * Esecuzione del cursore su db2 ed esecuzione della proc di inserimento in oracle
	 */
	public void Execute() throws Exception {
		
		try {
			
				if (this._truncStatement!=null) {
					OraConnection.InfoLog(this.getClass().toString()+".Execute", "Esecuzione dello script iniziale.");
					
					CallableStatement s = OraConnection.getConnection().prepareCall(this._truncStatement);
					s.execute();
				}				
			
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Esecuzione sql su database sorgente.");
				
			// Verifica quanti parametri sono presenti nella query
			int numberOfChar = numberOfParameter(_sql);	// Numero di parametri
			String sParm;								// Parametro recuperato
			int j;
			String sValue;								// Valore del parametro
			String sType;								// Tipo di parametro

			for (int i=1;i<=numberOfChar;i++) {
				// Recupero il parametro dalla tabella parametri step
				sParm = OraConnection.getStepParm("#"+i+"#");
				// Splitto il parametro
				j = sParm.indexOf('|');
				sValue = sParm.substring(0, j);
				sType = sParm.substring(j+1, sParm.length());
				
				if (sType.compareTo("VARCHAR2")==0) {
					_stmt.setString(i, sValue);
				}
				else if (sType.compareTo("NUMBER")==0) {
					_stmt.setBigDecimal(i, new BigDecimal(sValue));
				}
				else if (sType.compareTo("DATE")==0) {
					_stmt.setDate(i, DateToSql(StringToDate(sValue, "dd/MM/yyyy")));
				}
			}
							
			// Esecuzione dello statement db2 e loop fra il risultato
			ResultSet rs = _stmt.executeQuery();
						
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Sql eseguito correttamente.");
		
			int i=0;
			int n=0;
			while (rs.next()) {
				i++;
				
				//OraConnection.InfoLog(this.getClass().toString()+".Execute", "Elaborazione record n° " + i);
				
				if (i==1000) {
					n += i;
					i = 0;
					OraConnection.InfoLog(this.getClass().toString()+".Execute", "Elaborazione record n° " + n);
				}

				_meta.Call(rs);
				
				//OraConnection.InfoLog(this.getClass().toString()+".Execute", "Esecuzione procedura di caricamentoe eseguita.");
				
			}
			
		}
		catch (SQLException e) {
			OraConnection.LogAppException(this.getClass().toString()+".Execute", getStackTrace(e));
		}
		
	}
	
	private static String getStackTrace(Throwable throwable) {
	    Writer writer = new StringWriter();
	    PrintWriter printWriter = new PrintWriter(writer);
	    throwable.printStackTrace(printWriter);
	    return writer.toString();
	}
	
	private int numberOfParameter(String s) {
	   int n = 0;
		
	   for (int i = 0; i < s.length(); i ++) {
	      if (s.charAt(i) == '?') n++;
	   }
	   return n;
	}
	
	public static java.util.Date StringToDate(String date, String frm) {
		
		try {
            if (date == null) return null;
            SimpleDateFormat df = new SimpleDateFormat(frm);        
            return df.parse(date); 
        } catch(ParseException e){
            return null;            
        }
		
	}
	
	/**
	 * Converte una data in un Data.sql
	 **/
	public static java.sql.Date DateToSql(java.util.Date d) {
		
		if (d==null)
			return null;
		else
			return new java.sql.Date(d.getTime());
		
	}
	
	public static void Execute(int idStep) throws Exception {
		
			Cursor c = new Cursor(idStep);
			c.Execute();
		//}
		/*catch (Exception e) {
			System.out.println(e.getMessage());
		}*/
	}
	
}
