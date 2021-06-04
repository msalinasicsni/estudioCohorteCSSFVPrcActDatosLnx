package ni.com.sts.estudioCohorteCSSFV.util;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


@SuppressWarnings({"rawtypes","unchecked"})
public class Propiedades
{

    private List propertyFromFileSystem;
    private List propertyFromClassPath;
    private List basePath;
    private CompositeConfiguration config;
    //public static final String BASEPATH = "/opt/procesos/actualizacionDatos"; //"/ni/com/sts/propiedadesGeneral/";
    public static final String BASEPATH = "/ni/com/sts/propiedadesGeneral";
    //public static final String BASEPATH = "/opt/procesos/actualizacionDatos"; //"C:/procesos/actualizacionDatos"; //para windows
    //private final Logger logger = Logger.getLogger(this.getClass());

    public Propiedades()
    {
        config = new CompositeConfiguration();
        propertyFromFileSystem = new ArrayList();
        propertyFromClassPath = new ArrayList();
        basePath = new ArrayList();
    }

    public void addPropertyFilefromClassPath(String fileName)
    {
        loadPropertyfromClasspath(fileName);
    }

    public void addPropertyFilefromFileSystem(String fileName, String basePath)
    {
        loadPropertyfromFileSystem(fileName, basePath);
    }

    public void addPropertyFilefromFileSystem(String fileName)
    {

        propertyFromClassPath.add(fileName);
        //loadPropertyfromFileSystem(fileName, defaultDir() + "/ni/com/sts/propertiesGenerales/");
        loadPropertyfromFileSystem(fileName, userDir() + "/ni/com/sts/propertiesGenerales/");

    }

    private void loadPropertyfromClasspath(String fileName)
    {
        propertyFromFileSystem.add(fileName);
        PropertiesConfiguration configProperty = new PropertiesConfiguration();
        configProperty.setFileName(fileName);
        try
        {
            configProperty.load();
        }
        catch(ConfigurationException e)
        {
            String msg = "No se pudo cargar el archivo de propiedades del classpath[" + fileName + "]";
            e.printStackTrace();
            //logger.error(msg);
            throw new RuntimeException(msg);
        }
        config.addConfiguration(configProperty);
    }

    private void loadPropertyfromFileSystem(String fileName, String basePath)
    {

        propertyFromFileSystem.add(fileName);
        basePath = basePath.replace('/', System.getProperty("file.separator").charAt(0));
        this.basePath.add(basePath);

        File file = new File(basePath + fileName);
        PropertiesConfiguration configProperty = null;
        try
        {
            configProperty = new PropertiesConfiguration(file);
        }
        catch(ConfigurationException e)
        {
            String msg = "No se pudo cargar el archivo de propiedades del fileSystem[" + basePath + fileName + "]";
            //logger.error(msg);
            e.printStackTrace();
            throw new RuntimeException(msg);
        }
        configProperty.setFile(file);
        config.addConfiguration(configProperty);
    }

    public String userDir()
    {
    	 
        String userDir = System.getProperty("user.dir");
        //System.out.print("user.dir[" + userDir + "]");
        return userDir;
    }

    public String userHome()
    {
        String userHome = System.getProperty("user.home");
        //System.out.print("user.home[" + userHome + "]");
        return userHome;
    }

    public CompositeConfiguration getCompositeConfiguration()
    {
        return config;
    }

    public String defaultDir(){
    	//return "C://procesos//actualizacionDatos"; //para windows
    	return "/opt/procesos/actualizacionDatos"; 
    }

}
