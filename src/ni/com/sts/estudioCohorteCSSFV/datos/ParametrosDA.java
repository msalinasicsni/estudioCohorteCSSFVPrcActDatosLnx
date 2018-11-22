package ni.com.sts.estudioCohorteCSSFV.datos;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import ni.com.sts.estudioCohorteCSSFV.servicios.ParametroService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;

public class ParametrosDA extends ConnectionDAO implements ParametroService {
	private final Logger logger = Logger.getLogger(this.getClass());
	private Connection conn=null;
	private PreparedStatement  pstm = null;
	
	
	@Override
	public String getParametroByName(String nombre) {
		conn = getConection();
		String valor = null;
		ResultSet rs = null;
		try{
			String query = "Select valores from parametros_sistemas where nombre_parametro = ?";
			pstm = conn.prepareStatement(query);
			pstm.setString(1, nombre);
			rs = pstm.executeQuery();
			if (rs.next()){
				valor = rs.getString(1);
			}
		} catch (Exception e) {
		 		e.printStackTrace();
		} finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstm != null) {
                    pstm.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }
		return valor;
	}

}
