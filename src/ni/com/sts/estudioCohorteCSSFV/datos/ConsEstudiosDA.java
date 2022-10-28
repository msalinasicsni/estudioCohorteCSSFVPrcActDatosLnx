package ni.com.sts.estudioCohorteCSSFV.datos;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.axis.encoding.Base64;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import ni.com.sts.estudioCohorteCSSFV.dto.ConsentimientoDto;
import ni.com.sts.estudioCohorteCSSFV.modelo.ConsEstudios;
import ni.com.sts.estudioCohorteCSSFV.servicios.ConsEstudiosService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;

public class ConsEstudiosDA extends ConnectionDAO implements ConsEstudiosService {

	private final Logger logger = Logger.getLogger(this.getClass());
	private Connection connTransac=null;
	private CompositeConfiguration config;
	
	public ConsEstudiosDA(){
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
	public void AddConsEstudio(ConsEstudios dato) throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("INSERT INTO cons_estudios " +
            		"(fecha, codigo_expediente, codigo_consentimiento, asentchik, parteb, partec, parted, asentimientoesc, partee, partef, tipoparttrans, reactivacion, ahora, retirado) " +
            		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            pst.setDate(1, (Date)dato.getFecha());
            pst.setInt(2, dato.getCodigoExpediente());
            pst.setInt(3, dato.getCodigoConsentimiento());
            pst.setInt(4, dato.getAsentchik());
            pst.setInt(5, dato.getParteb());
            pst.setInt(6, dato.getPartec());
            pst.setInt(7, dato.getParted());
            pst.setInt(8, dato.getAsentimientoesc());
            pst.setInt(9, dato.getPartee());
            pst.setInt(10, dato.getPartef());
            pst.setString(11, dato.getTipoparttrans());
            pst.setString(12, dato.getReactivacion());
            pst.setDate(13, (Date)dato.getAhora());
            pst.setString(14, String.valueOf(dato.getRetirado()));
            
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al guardar el registro :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
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
            st.addBatch("DELETE FROM cons_estudios_tmp");
            st.addBatch("INSERT INTO cons_estudios_tmp(fecha, codigo_expediente, codigo_consentimiento, asentchik, parteb, partec, parted, asentimientoesc, partee, partef, tipoparttrans, reactivacion, ahora, retirado) " +
            		"SELECT fecha, codigo_expediente, codigo_consentimiento, asentchik, parteb, partec, parted, asentimientoesc, partee, partef, tipoparttrans, reactivacion, ahora, retirado " +
            		"FROM cons_estudios");
            st.addBatch("DELETE FROM cons_estudios");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al crear respaldo :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
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
	public void limpiarConsEstudiosTmp() throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("DELETE FROM cons_estudios_tmp");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al eliminar temporal cons estudios :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
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
	public List<ConsEstudios> getConsEstudiosFromODBC() throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		Connection connODBC = null;
		List<ConsEstudios> consEstudiosList = new ArrayList<ConsEstudios>();
        try {
        	 //connODBC = getODBCConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.scancartas")),"","");//getODBCConnection(config.getString("dns.bdScanCartas"));
        	 connODBC = getUcanaccessConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.scancartas")));
        	 String query = "select Fecha, codigo, cons, asentChik, ParteB, ParteC, ParteD, asentimientoesc, ParteE, ParteF, TipoPartTrans, " +
		      		"Reactivacion, ahora, retirado " +
			      		"from scan";
        	stmt = connODBC.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  ConsEstudios consEstudio = new ConsEstudios();
		    	  consEstudio.setFecha(rs.getDate("Fecha"));
		    	  consEstudio.setCodigoExpediente(rs.getInt("codigo"));
		    	  consEstudio.setCodigoConsentimiento(rs.getShort("cons"));
		    	  consEstudio.setAsentchik(rs.getShort("asentChik"));
		    	  consEstudio.setParteb(rs.getShort("ParteB"));
		    	  consEstudio.setPartec(rs.getShort("ParteC"));
		    	  consEstudio.setParted(rs.getShort("ParteD"));
		    	  consEstudio.setAsentimientoesc(rs.getShort("asentimientoesc"));
		    	  consEstudio.setPartee(rs.getShort("ParteE"));
		    	  consEstudio.setPartef(rs.getShort("ParteF"));
		    	  consEstudio.setTipoparttrans(rs.getString("TipoPartTrans"));
		    	  consEstudio.setReactivacion(rs.getString("Reactivacion"));
		    	  consEstudio.setAhora(rs.getDate("ahora"));
		    	  consEstudio.setRetirado(rs.getBoolean("retirado")==true?'1':'0');
		    	  
		    	  //logger.info(consEstudio.getCodigoExpediente() + " " + consEstudio.getCodigoConsentimiento() + " " + consEstudio.getFecha());		    	  
		    	  consEstudiosList.add(consEstudio);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
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
        return consEstudiosList;
	}
	
	@Override
	public List<ConsEstudios> getConsEstudiosFromServerBDEstudios() throws Exception {
		List<ConsEstudios> consEstudiosList = new ArrayList<ConsEstudios>();
		try {
			String CONSENTIMIENTOS = config.getString("CONSENTIMIENTOS");
			String user = config.getString("USER");
			String pwd = config.getString("PWD");
			URL url = new URL(CONSENTIMIENTOS);
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
			
			List<ConsentimientoDto> consentimientoDto = new ArrayList<ConsentimientoDto>();
		    Type founderListType = founderListType = new TypeToken<ArrayList<ConsentimientoDto>>(){}.getType();
		    consentimientoDto = new Gson().fromJson(response.toString(), founderListType);
		    
		    for (int i = 0; i < consentimientoDto.size(); i++) {
		    	ConsEstudios consEstudio = new ConsEstudios();
		    	Calendar cal = Calendar.getInstance();
		    	if (consentimientoDto.get(i).getFecha() != null && consentimientoDto.get(i).getFecha() != "") {
		    		long longDate = Long.valueOf(consentimientoDto.get(i).getFecha());
			    	Date date = new Date(longDate);
			    	consEstudio.setFecha(date);
		    	}
		    	consEstudio.setCodigoExpediente(consentimientoDto.get(i).getCodigo());
		    	if (consentimientoDto.get(i).getCons() != null) {
		    		consEstudio.setCodigoConsentimiento(consentimientoDto.get(i).getCons());
		    	}
		    	if (consentimientoDto.get(i).getAsentChik() != null ) {
		    		consEstudio.setAsentchik((short) consentimientoDto.get(i).getAsentChik());
		    	} else {
		    		consEstudio.setAsentchik((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getParteB() != null) {
		    		consEstudio.setParteb(consentimientoDto.get(i).getParteB().shortValue());
		    	} else {
		    		consEstudio.setParteb((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getParteC() != null) {
		    		consEstudio.setPartec(consentimientoDto.get(i).getParteC().shortValue());
		    	} else {
		    		consEstudio.setPartec((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getParteD() != null) {
		    		consEstudio.setParted(consentimientoDto.get(i).getParteD().shortValue());
		    	} else {
		    		consEstudio.setParted((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getAsentimientoEsc() != null) {
		    		consEstudio.setAsentimientoesc(consentimientoDto.get(i).getAsentimientoEsc().shortValue());
		    	} else {
		    		consEstudio.setAsentimientoesc((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getParteE() != null) {
		    		consEstudio.setPartee(consentimientoDto.get(i).getParteE().shortValue());
		    	} else {
		    		consEstudio.setPartee((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getParteF() != null) {
		    		consEstudio.setPartef(consentimientoDto.get(i).getParteF().shortValue());
		    	} else {
		    		consEstudio.setPartef((short) 0);
		    	}
		    	if (consentimientoDto.get(i).getTipoPartTrans() != null) {
		    		consEstudio.setTipoparttrans(consentimientoDto.get(i).getTipoPartTrans());
		    	} else {
		    		consEstudio.setTipoparttrans(null);
		    	}
		    	if (consentimientoDto.get(i).getReactivacion() != null && !consentimientoDto.get(i).getReactivacion().trim().equals("")) {
		    		consEstudio.setReactivacion(consentimientoDto.get(i).getReactivacion());
		    	} else {
		    		consEstudio.setReactivacion(null);
		    	}
		    	
		    	if (consentimientoDto.get(i).getAhora() != null && consentimientoDto.get(i).getAhora() != "") {
		    		long longDate = Long.valueOf(consentimientoDto.get(i).getAhora());
			    	Date dateAhora = new Date(longDate);
			    	consEstudio.setAhora(dateAhora);
		    	} else {
		    		consEstudio.setAhora(null);
		    	}
		    	
		    	consEstudio.setRetirado(consentimientoDto.get(i).getRetirado() == true ? '1' : '0');
		    	
		    	consEstudiosList.add(consEstudio);
		    }
		    br.close();
			http.disconnect();
		} catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        } 
		return consEstudiosList;
	}
	
	@Override
	public List<ConsEstudios> getConsEstudiosFromBDEstudios(Connection connNoTransacMySql) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		List<ConsEstudios> consEstudiosList = new ArrayList<ConsEstudios>();
        try {
        	 String query = "select Fecha, codigo, cons, asentChik, ParteB, ParteC, ParteD, asentimientoesc, ParteE, ParteF, TipoPartTrans, " +
		      		"Reactivacion, ahora, retirado " +
			      		"from scan";
        	stmt = connNoTransacMySql.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  ConsEstudios consEstudio = new ConsEstudios();
		    	  consEstudio.setFecha(rs.getDate("Fecha"));
		    	  consEstudio.setCodigoExpediente(rs.getInt("codigo"));
		    	  consEstudio.setCodigoConsentimiento(rs.getShort("cons"));
		    	  consEstudio.setAsentchik(rs.getShort("asentChik"));
		    	  consEstudio.setParteb(rs.getShort("ParteB"));
		    	  consEstudio.setPartec(rs.getShort("ParteC"));
		    	  consEstudio.setParted(rs.getShort("ParteD"));
		    	  consEstudio.setAsentimientoesc(rs.getShort("asentimientoesc"));
		    	  consEstudio.setPartee(rs.getShort("ParteE"));
		    	  consEstudio.setPartef(rs.getShort("ParteF"));
		    	  consEstudio.setTipoparttrans(rs.getString("TipoPartTrans"));
		    	  consEstudio.setReactivacion(rs.getString("Reactivacion"));
		    	  consEstudio.setAhora(rs.getDate("ahora"));
		    	  consEstudio.setRetirado(rs.getBoolean("retirado")==true?'1':'0');
		    	  
		    	  //logger.info(consEstudio.getCodigoExpediente() + " " + consEstudio.getCodigoConsentimiento() + " " + consEstudio.getFecha());		    	  
		    	  consEstudiosList.add(consEstudio);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
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
        return consEstudiosList;
	}

	@Override
	public void deshacerRespaldo(Connection connNoTransac) throws Exception {
		Statement st = null;
        try {
        	logger.info("deshacerRespaldo ConsEstudiosDA");
            st = connNoTransac.createStatement();
            st.addBatch("DELETE FROM cons_estudios");
            st.addBatch("INSERT INTO cons_estudios(fecha, codigo_expediente, codigo_consentimiento, asentchik, parteb, partec, parted, asentimientoesc, partee, partef, tipoparttrans, reactivacion, ahora, retirado) " +
            		"SELECT fecha, codigo_expediente, codigo_consentimiento, asentchik, parteb, partec, parted, asentimientoesc, partee, partef, tipoparttrans, reactivacion, ahora, retirado " +
            		"FROM cons_estudios_tmp");
            //st.addBatch("DELETE FROM cons_estudios_tmp");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
            logger.info("Committed " + counts.length + " updates");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al deshacer respaldo :: ConsEstudiosDA" + "\n" + e.getMessage(),e);
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

}
