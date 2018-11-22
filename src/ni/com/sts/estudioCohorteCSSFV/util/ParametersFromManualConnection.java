package ni.com.sts.estudioCohorteCSSFV.util;

import java.util.HashMap;
import java.util.Map;

public class ParametersFromManualConnection {

    private String databaseName;
    private String hostName;
    private Integer port;
    private String databaseUserName;
    private String password;
    private String minlimit;
    private String maxlimit;

    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<String, String>();
        map.put( "databaseName....." , databaseName );
        map.put( "hostName........." , hostName );
        map.put( "port............." , String.valueOf(port) );
        map.put( "databaseUserName." , databaseUserName );
        map.put( "password........." , password );
        map.put( "minlimit........." , minlimit );
        map.put( "maxlimit........." , maxlimit );
        return map;
    }
    
    
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	public String getDatabaseUserName() {
		return databaseUserName;
	}
	public void setDatabaseUserName(String databaseUserName) {
		this.databaseUserName = databaseUserName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getMaxlimit() {
		return maxlimit;
	}
	public void setMaxlimit(String maxlimit) {
		this.maxlimit = maxlimit;
	}
	public String getMinlimit() {
		return minlimit;
	}
	public void setMinlimit(String minlimit) {
		this.minlimit = minlimit;
	}
}
