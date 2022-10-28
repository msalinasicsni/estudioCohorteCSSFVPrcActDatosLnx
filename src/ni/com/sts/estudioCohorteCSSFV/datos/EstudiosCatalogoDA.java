package ni.com.sts.estudioCohorteCSSFV.datos;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.axis.encoding.Base64;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import ni.com.sts.estudioCohorteCSSFV.dto.TipoConsentimientoDto;
import ni.com.sts.estudioCohorteCSSFV.modelo.EstudioCatalogo;
import ni.com.sts.estudioCohorteCSSFV.servicios.EstudiosCatalogoService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;

public class EstudiosCatalogoDA extends ConnectionDAO implements EstudiosCatalogoService {

	private CompositeConfiguration config;
	private final Logger logger = Logger.getLogger(this.getClass());
	private Connection connTransac=null;
	
	public EstudiosCatalogoDA(){
		config = UtilProperty.getConfigurationfromExternalFile("estudioCohorteCSSFVAD.properties");
		UtilLog.setLog(config.getString("estudioCohorteCSSFVAD.log"));
	}
	
	@Override
	public void setConnTransac(Connection conn) {
		this.connTransac = conn;
	}

	@Override
	public Connection getConnTransac() {
		return connTransac;
	}
	
	@Override
	public void AddEstudio(EstudioCatalogo dato) throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("INSERT INTO estudio_catalogo(cod_estudio, desc_estudio) values(?,?)");
            pst.setInt(1, dato.getCodEstudio());
            pst.setString(2, dato.getDescEstudio());
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al guardar el registro :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
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
	public void limpiarEstudios() throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("DELETE FROM estudio_catalogo");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al eliminar todos los registros :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
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
            st.addBatch("DELETE FROM estudio_catalogo_tmp");
            st.addBatch("INSERT INTO estudio_catalogo_tmp(cod_estudio,desc_estudio) SELECT cod_estudio,desc_estudio FROM estudio_catalogo");
            st.addBatch("DELETE FROM estudio_catalogo");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
        }catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al crear respaldo :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
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
        	logger.info("deshacerRespaldo EstudiosCatalogoDA");
            st = connNoTransac.createStatement();
            st.addBatch("DELETE FROM estudio_catalogo");
            st.addBatch("INSERT INTO estudio_catalogo(cod_estudio,desc_estudio) SELECT cod_estudio,desc_estudio FROM estudio_catalogo_tmp");
            //st.addBatch("DELETE FROM estudio_catalogo_tmp");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
            logger.info("Committed " + counts.length + " updates");
        }catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al deshacer respaldo :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
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
	public void limpiarEstudioTmp() throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("DELETE FROM estudio_catalogo_tmp");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al eliminar catalogo temporal escuelas :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
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
	public List<EstudioCatalogo> getEstudiosFromODBC() throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		Connection connODBC = null;
		List<EstudioCatalogo> estudiosList = new ArrayList<EstudioCatalogo>();
        try {
        	//connODBC = getODBCConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.scancartas")),"","");//getODBCConnection(config.getString("dns.bdScanCartas"));
        	connODBC = getUcanaccessConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.scancartas")));
        	String query = "select cons, desc_cons " +
		      		"from tipCons";
        	stmt = connODBC.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  EstudioCatalogo estudio = new EstudioCatalogo();
		    	  estudio.setCodEstudio(rs.getInt("cons"));
		    	  estudio.setDescEstudio(rs.getString("desc_cons"));		    	  
		    	  estudiosList.add(estudio);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
            	rs.close();
	        	stmt.close();
	        if (!connODBC.isClosed()){
	        	connODBC.close();
	        	System.out.println("Conexión cerrada");
	        }
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        } 
		
	      return estudiosList;
	}
	
	@Override
	public List<EstudioCatalogo> getEstudiosFromServerDBEstudios() throws Exception {
		List<EstudioCatalogo> estudiosList = new ArrayList<EstudioCatalogo>();
		try {
			String TIPOSCONSENTIMIENTOS = config.getString("TIPOSCONSENTIMIENTOS");
			String user = config.getString("USER");
			String pwd = config.getString("PWD");
			URL url = new URL(TIPOSCONSENTIMIENTOS);
			String userPassword = user + ":" + pwd;
			String encodedAuth = Base64.encode(userPassword.getBytes(StandardCharsets.UTF_8));
			String authHeaderValue = "Basic " + encodedAuth;
			HttpURLConnection http = (HttpURLConnection)url.openConnection();
			http.setRequestProperty("Authorization", authHeaderValue);
			
			BufferedReader br = null;
			if (http.getResponseCode() == 200) {
			    br = new BufferedReader(new InputStreamReader(http.getInputStream(), StandardCharsets.UTF_8));
			} else {
			    br = new BufferedReader(new InputStreamReader(http.getErrorStream(), StandardCharsets.UTF_8));
			}
			StringBuilder response = new StringBuilder();
			String currentLine;
			while ((currentLine = br.readLine()) != null)
				response.append(currentLine);
			
			List<TipoConsentimientoDto> tipoConsentimientoDto = new ArrayList<TipoConsentimientoDto>();
		    Type founderListType = founderListType = new TypeToken<ArrayList<TipoConsentimientoDto>>(){}.getType();
		    tipoConsentimientoDto = new Gson().fromJson(response.toString(), founderListType);
		    
		    for (int i = 0; i < tipoConsentimientoDto.size(); i++) {
		    	EstudioCatalogo estudio = new EstudioCatalogo();
		    	estudio.setCodEstudio(tipoConsentimientoDto.get(i).getCons());
		    	estudio.setDescEstudio(tipoConsentimientoDto.get(i).getDescCons());		    	  
		    	estudiosList.add(estudio);
			}
		    br.close();
			http.disconnect();
		} catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        }
		return estudiosList;
	}
	
	@Override
	public List<EstudioCatalogo> getEstudiosFromDBEstudios(Connection connNoTransac) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		List<EstudioCatalogo> estudiosList = new ArrayList<EstudioCatalogo>();
        try {
        	String query = "select cons, desc_cons " +
		      		"from tipo_consentimiento";
        	stmt = connNoTransac.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  EstudioCatalogo estudio = new EstudioCatalogo();
		    	  estudio.setCodEstudio(rs.getInt("cons"));
		    	  estudio.setDescEstudio(rs.getString("desc_cons"));		    	  
		    	  //logger.info(estudio.getCodEstudio() + " " + estudio.getDescEstudio());
		    	  //System.out.println(estudio.getCodEstudio() + " " + estudio.getDescEstudio());
		    	  estudiosList.add(estudio);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: EstudiosCatalogoDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } finally {
            try {
            	rs.close();
	        	stmt.close();
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        } 
		
	      return estudiosList;
	}

}
