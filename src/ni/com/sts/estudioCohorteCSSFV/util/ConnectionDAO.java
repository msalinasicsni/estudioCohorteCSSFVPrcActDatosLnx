package ni.com.sts.estudioCohorteCSSFV.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import net.ucanaccess.jdbc.UcanaccessDriver;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

public class ConnectionDAO {
	private final Logger logger = Logger.getLogger(this.getClass());
	private CompositeConfiguration config;
	public ConnectionDAO(){
		config = UtilProperty.getConfigurationfromExternalFile("estudioCohorteCSSFVAD.properties");
		UtilLog.setLog(config.getString("estudioCohorteCSSFVAD.log"));	
		logger.debug("Constructor ConnectionDAO():end");
	}
	
	public Connection getConection()
	{		
		logger.debug("getConection():start");
		try {
			ParametersFromManualConnection parametersConnection = new ParametersFromManualConnection();
			
			logger.debug("database.host ["+config.getString("database.host")+"]");
			parametersConnection.setHostName(config.getString("database.host"));
			
			logger.debug("database.name ["+config.getString("database.name")+"]");
			parametersConnection.setDatabaseName(config.getString("database.name"));
			
			logger.debug("database.user ["+config.getString("database.user")+"]");
			parametersConnection.setDatabaseUserName(config.getString("database.user"));
			
			logger.debug("database.password ["+config.getString("database.password")+"]");
			parametersConnection.setPassword(config.getString("database.password"));
			
			logger.debug("database.port ["+config.getString("database.port")+"]");
			parametersConnection.setPort(Integer.valueOf(config.getString("database.port")));
			
			logger.debug("database.maxlimit ["+config.getString("database.maxlimit")+"]");
			parametersConnection.setMaxlimit(config.getString("database.maxlimit"));
			
			logger.debug("database.minlimit ["+config.getString("database.minlimit")+"]");
			parametersConnection.setMinlimit(config.getString("database.minlimit"));
			return getConnectionManualToOracleDB(parametersConnection);
		} catch (Exception e1) {			
			logger.error(" No se pudo obtener una conexión ", e1);
			return null;
		}finally{
			logger.debug("getConection():end");
		}
	}

	
	private Connection getConnectionManualToOracleDB(ParametersFromManualConnection parametros) throws SQLException {

		final Properties props = new Properties();
		props.put("user", parametros.getDatabaseUserName());
		props.put("password", parametros.getPassword());
		try {
			Class.forName("org.postgresql.Driver").newInstance();
		} catch (final ClassNotFoundException ex) {
			ex.printStackTrace();
			logger.error("ClassNotFoundException", ex);
		} catch (final IllegalAccessException ex) {
			ex.printStackTrace();
			logger.error("IllegalAccessException", ex);
		} catch (final InstantiationException ex) {
			ex.printStackTrace();
			logger.error("InstantiationException", ex);
		}

		final StringBuffer url = new StringBuffer();
		url.append("jdbc:postgresql://");
		url.append(parametros.getHostName());
		url.append(":");
		url.append(parametros.getPort().toString());
		url.append("/");
		url.append(parametros.getDatabaseName());
		return DriverManager.getConnection(url.toString(), props);
	}
	
	public Connection getODBCConnection(String dns, String password, String username) throws Exception {
	    String driver = "sun.jdbc.odbc.JdbcOdbcDriver";
	    String url = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + dns+";Pwd="+password+";";
	    logger.info("establecer conexión JDBC");
	    logger.debug("url: "+url);
	    logger.debug("password: "+password);
	    //String url = "jdbc:odbc:"+dns;
	    //String url2 = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:/Users/FIRSTICT/Documents/alumnos.accdb";
	    Class.forName(driver).newInstance();; // load JDBC-ODBC driver
	    java.sql.DriverManager.setLogStream(java.lang.System.out);
	    return DriverManager.getConnection(url, username, password);
	  }
	
	public Connection getUcanaccessConnection(String dns) throws Exception{	
	    Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");//Loading Driver
	    return DriverManager.getConnection("jdbc:ucanaccess://"+dns);//Establishing Connection
    } 

	public Connection getUcanaccessCryptedConnection(String dns, String password, String username) throws Exception{
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver").newInstance();//Loading Driver
		//return DriverManager.getConnection("jdbc:ucanaccess://words.accdb;jackcessOpener=CryptCodecOpener", "user", "pass");
		return DriverManager.getConnection("jdbc:ucanaccess://"+dns+";jackcessOpener=ni.com.sts.estudioCohorteCSSFV.util.CryptCodecOpener", username, password);//Establishing Connection
	}
	
	/* Conexión a MySql -> 3/12/2019 - SC ************************************************************************************/
	public Connection getConectionMySql() {
		logger.debug("getConectionMySql():start");
		try {
			ParametersFromManualConnection parametersConnection = new ParametersFromManualConnection();
		
			logger.debug("databaseMySql.host [" + config.getString("databaseMySql.host") + "]");
			parametersConnection.setHostName(config.getString("databaseMySql.host"));
			//logger.debug("databaseMySql.host [" + dataBaseHost + "]");
			//parametersConnection.setHostName(dataBaseHost);

			logger.debug("databaseMySql.name [" + config.getString("databaseMySql.name") + "]");
			parametersConnection.setDatabaseName(config.getString("databaseMySql.name"));
			//logger.debug("databaseMySql.name [" + dataBaseName + "]");
			//parametersConnection.setDatabaseName(dataBaseName);

			logger.debug("databaseMySql.user [" + config.getString("databaseMySql.user") + "]");
			parametersConnection.setDatabaseUserName(config.getString("databaseMySql.user"));

			logger.debug("databaseMySql.password [" + config.getString("databaseMySql.password") + "]");
			parametersConnection.setPassword(config.getString("databaseMySql.password"));

			logger.debug("databaseMySql.port [" + config.getString("databaseMySql.port") + "]");
			parametersConnection.setPort(Integer.valueOf(config.getString("databaseMySql.port")));

			logger.debug("databaseMySql.maxlimit [" + config.getString("databaseMySql.maxlimit") + "]");
			parametersConnection.setMaxlimit(config.getString("databaseMySql.maxlimit"));

			logger.debug("databaseMySql.minlimit [" + config.getString("databaseMySql.minlimit") + "]");
			parametersConnection.setMinlimit(config.getString("databaseMySql.minlimit"));
			return getConnectionManualToMySqlDB(parametersConnection);
		} catch (Exception e1) {
			logger.error(" No se pudo obtener una conexión ", e1);
			return null;
		} finally {
			logger.debug("getConectionMySql():end");
		}
	}
	
	private Connection getConnectionManualToMySqlDB(ParametersFromManualConnection parametros) throws SQLException {

		final Properties props = new Properties();
		props.put("user", parametros.getDatabaseUserName());
		props.put("password", parametros.getPassword());
		try {
			//Class.forName("org.postgresql.Driver").newInstance();
			Class.forName("org.mysql.jdbc.Driver").newInstance();
		} catch (final ClassNotFoundException ex) {
			ex.printStackTrace();
			logger.error("ClassNotFoundException", ex);
		} catch (final IllegalAccessException ex) {
			ex.printStackTrace();
			logger.error("IllegalAccessException", ex);
		} catch (final InstantiationException ex) {
			ex.printStackTrace();
			logger.error("InstantiationException", ex);
		}

		final StringBuffer url = new StringBuffer();
		//url.append("jdbc:postgresql://");
		url.append("jdbc:mysql://");
		url.append(parametros.getHostName());
		url.append(":");
		url.append(parametros.getPort().toString());
		url.append("/");
		url.append(parametros.getDatabaseName());
		return DriverManager.getConnection(url.toString(), props);
	}
}