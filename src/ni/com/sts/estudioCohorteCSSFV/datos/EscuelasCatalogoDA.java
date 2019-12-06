package ni.com.sts.estudioCohorteCSSFV.datos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

import ni.com.sts.estudioCohorteCSSFV.modelo.EscuelaCatalogo;
import ni.com.sts.estudioCohorteCSSFV.servicios.EscuelasCatalogoService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;

public class EscuelasCatalogoDA extends ConnectionDAO implements EscuelasCatalogoService {

	private final Logger logger = Logger.getLogger(this.getClass());
	private Connection connTransac=null;
	private CompositeConfiguration config;
	public EscuelasCatalogoDA(){
		config = UtilProperty.getConfigurationfromExternalFile("estudioCohorteCSSFVAD.properties");
		UtilLog.setLog(config.getString("estudioCohorteCSSFVAD.log"));
	}
	
	@Override
	public Connection getConnTransac() {
		return connTransac;
	}	

	@Override
	public void setConnTransac(Connection conn) {
		this.connTransac = conn;
	}
	
	@Override
	public void AddEscuela(EscuelaCatalogo dato) throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("INSERT INTO escuela_catalogo(cod_escuela, descripcion) values(?,?)");
            pst.setInt(1, dato.getCodEscuela());
            pst.setString(2, dato.getDescripcion());
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al guardar el registro :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }
	}

	@Override
	public void limpiarEscuelas() throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("DELETE FROM escuela_catalogo");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al eliminar todos los registros :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }

	}

	@Override
	public void crearRespaldo(Connection connNoTransac) throws Exception {
		Statement st = null;
        try {
            st = connNoTransac.createStatement();
            st.addBatch("DELETE FROM escuela_catalogo_tmp");
            st.addBatch("INSERT INTO escuela_catalogo_tmp(cod_escuela,descripcion) SELECT cod_escuela,descripcion FROM escuela_catalogo");
            st.addBatch("DELETE FROM escuela_catalogo");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al crear respaldo :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }

	}
	
	@Override
	public void deshacerRespaldo(Connection connNoTransac) throws Exception {
		Statement st = null;
        try {
        	logger.info("deshacerRespaldo EscuelasCatalogoDA");
            st = connNoTransac.createStatement();
            st.addBatch("DELETE FROM escuela_catalogo");
            st.addBatch("INSERT INTO escuela_catalogo(cod_escuela,descripcion) SELECT cod_escuela,descripcion FROM escuela_catalogo_tmp");
            //st.addBatch("DELETE FROM escuela_catalogo_tmp");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
            logger.info("Committed " + counts.length + " updates");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al deshacer respaldo :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }

	}
	
	@Override
	public void limpiarEscuelasTmp() throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("DELETE FROM escuela_catalogo_tmp");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al eliminar catalogo temporal escuelas :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }

	}

	@Override
	public List<EscuelaCatalogo> getEscuelasFromODBC() throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		Connection connODBC = null;
		List<EscuelaCatalogo> escuelasList = new ArrayList<EscuelaCatalogo>();
        try {
        	//connODBC = getODBCConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.pdvi")),config.getString("dns.bdpdvi.password"),"");//getODBCConnection(config.getString("dns.bdpdvi"));
        	connODBC = getUcanaccessCryptedConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.pdvi")),config.getString("dns.bdpdvi.password"),"");
        	 String query = "select escuela, escuela_descripcion " +
			      		"from Escuelas_DGCatalogo";
        	stmt = connODBC.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  EscuelaCatalogo escuela = new EscuelaCatalogo();
		    	  escuela.setCodEscuela(rs.getInt("escuela"));
		    	  escuela.setDescripcion(rs.getString("escuela_descripcion"));		    	  
		    	  //logger.info(escuela.getCodEscuela() + " " + escuela.getDescripcion());
		    	  //System.out.println(escuela.getCodEscuela() + " " + escuela.getDescripcion());
		    	  escuelasList.add(escuela);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
            	if (rs != null)
            		rs.close();
            	if (stmt != null)
            		stmt.close();
	        if (!connODBC.isClosed()){
	        	connODBC.close();
	        	System.out.println("Conexión cerrada");
	        }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        } 
		
	      return escuelasList;
	}

	@Override
	public List<EscuelaCatalogo> getEscuelasFromBDEstudios(Connection connNoTransac) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		List<EscuelaCatalogo> escuelasList = new ArrayList<EscuelaCatalogo>();
        try {
        	 String query = "select escuela, escuela_descripcion " +
			      		"from escuelas";
        	stmt = connNoTransac.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  EscuelaCatalogo escuela = new EscuelaCatalogo();
		    	  escuela.setCodEscuela(rs.getInt("escuela"));
		    	  escuela.setDescripcion(rs.getString("escuela_descripcion"));		    	  
		    	  //logger.info(escuela.getCodEscuela() + " " + escuela.getDescripcion());
		    	  //System.out.println(escuela.getCodEscuela() + " " + escuela.getDescripcion());
		    	  escuelasList.add(escuela);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: EscuelasCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
            	if (rs != null)
            		rs.close();
            	if (stmt != null)
            		stmt.close();
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        } 
		
	      return escuelasList;
	}
	
}
