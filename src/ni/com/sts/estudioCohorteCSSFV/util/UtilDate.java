package ni.com.sts.estudioCohorteCSSFV.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UtilDate {
	
	public static String DateToString(Date fecha, String formato){
		SimpleDateFormat sdf = new SimpleDateFormat(formato);
		return sdf.format(fecha);
	}

	public static Date StringToDate(String fecha, String formato) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat(formato);
		return sdf.parse(fecha);
	}
}
