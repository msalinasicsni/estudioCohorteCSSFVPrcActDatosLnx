package ni.com.sts.estudioCohorteCSSFV.util;


import org.apache.commons.configuration.CompositeConfiguration;

public class UtilProperty
{

 public UtilProperty()
 {
 }

 public static CompositeConfiguration getConfiguration(String fileNamefromFileSystem, String fileNamefromClasspath)
 {
	 
	 Propiedades propiedades = new Propiedades();
	 propiedades.addPropertyFilefromFileSystem(fileNamefromFileSystem);
	 propiedades.addPropertyFilefromClassPath(fileNamefromClasspath);
	 return propiedades.getCompositeConfiguration();
 }

 public static CompositeConfiguration getConfigurationfromExternalFile(String fileNamefromFileSystem)
 {
     Propiedades propiedades = new Propiedades();
     propiedades.addPropertyFilefromFileSystem(fileNamefromFileSystem);
     return propiedades.getCompositeConfiguration();
 }

 public static CompositeConfiguration getConfigurationfromClasspath(String fileNamefromClasspath)
 {
     Propiedades propiedades = new Propiedades();
     propiedades.addPropertyFilefromClassPath(fileNamefromClasspath);
     return propiedades.getCompositeConfiguration();
 }
}
