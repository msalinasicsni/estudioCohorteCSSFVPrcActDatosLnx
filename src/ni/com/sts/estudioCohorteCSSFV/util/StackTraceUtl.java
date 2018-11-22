package ni.com.sts.estudioCohorteCSSFV.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StackTraceUtl {
	
    public static String getStackTrace(Throwable exception)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        return sw.toString();
    }
}
