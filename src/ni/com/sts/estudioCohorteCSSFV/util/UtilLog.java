package ni.com.sts.estudioCohorteCSSFV.util;

import org.apache.log4j.PropertyConfigurator;

public class UtilLog {
	public static void setLog(String logFileName)
    {
        String log = Propiedades.BASEPATH + logFileName; //System.getProperty("user.dir") + logFileName;
        //System.out.println("Ruta Archiv conf Log ["+log+"]");
        PropertyConfigurator.configure(log);
    }
}
