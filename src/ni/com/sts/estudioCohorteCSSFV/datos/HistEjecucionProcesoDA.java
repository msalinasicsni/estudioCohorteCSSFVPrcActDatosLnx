package ni.com.sts.estudioCohorteCSSFV.datos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import ni.com.sts.estudioCohorteCSSFV.modelo.HistEjecucionProcesoAutomatico;
import ni.com.sts.estudioCohorteCSSFV.servicios.HistEjecucionProcesoService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.InfoResultado;

public class HistEjecucionProcesoDA extends ConnectionDAO implements
		HistEjecucionProcesoService {
	
	private final Logger logger = Logger.getLogger(this.getClass());
	private Connection conn=null;
	
	@Override
	public HistEjecucionProcesoAutomatico getEjecucionProcesoFechaHoy(String proceso) {
		conn = getConection();
		HistEjecucionProcesoAutomatico resultado = null;
		PreparedStatement  stm = null;
		ResultSet rs = null;
		try{
			String query = "select sec_ejecucion, fecha_ejecucion from histo_ejecucion_proceso "+
							"where proceso = ? and to_char(fecha_ejecucion,'ddMMyyyy') = to_char(CURRENT_DATE,'ddMMyyyy') ";
						
			stm = conn.prepareStatement(query);
			stm.setString(1, proceso);
			rs = stm.executeQuery();
			if (rs.next()){
				resultado = new HistEjecucionProcesoAutomatico();
				resultado.setSecEjecucion(rs.getInt(1));
				resultado.setFechaEjecucion(rs.getDate(2));								
			}
		} catch (Exception e) {
		 		e.printStackTrace();
		} finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stm != null) {
                    stm.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }
        return resultado;
	}

	@SuppressWarnings("static-access")
	public InfoResultado registrarEjecucionProceso(String proceso){
		InfoResultado infoResultado = new InfoResultado();
		PreparedStatement pst = null;		
        try {
        	conn = getConection();
            pst = conn.prepareStatement("INSERT INTO histo_ejecucion_proceso(fecha_ejecucion,proceso) VALUES(CURRENT_DATE,?)");
            pst.setString(1, proceso);
            pst.executeUpdate();

            infoResultado.setOk(true);
            infoResultado.setMensaje("Registro exitoso :: histo_ejecucion_openclinica");
        } catch (Exception e) {
            e.printStackTrace();
            infoResultado.setOk(false);
            infoResultado.setExcepcion(true);
            infoResultado.setGravedad(infoResultado.ERROR);
            infoResultado.setMensaje("Se ha producido un error al guardar el registro :: histo_ejecucion_openclinica" + "\n" + e.getMessage());
            infoResultado.setFuenteError("registrarEjecucionProceso");
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }

        return infoResultado;
	}
}
