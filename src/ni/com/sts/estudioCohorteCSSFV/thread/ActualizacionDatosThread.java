package ni.com.sts.estudioCohorteCSSFV.thread;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import ni.com.sts.estudioCohorteCSSFV.datos.ConsEstudiosDA;
import ni.com.sts.estudioCohorteCSSFV.datos.EscuelasCatalogoDA;
import ni.com.sts.estudioCohorteCSSFV.datos.EstudiosCatalogoDA;
import ni.com.sts.estudioCohorteCSSFV.datos.HistEjecucionProcesoDA;
import ni.com.sts.estudioCohorteCSSFV.datos.PacientesDA;
import ni.com.sts.estudioCohorteCSSFV.datos.ParametrosDA;
import ni.com.sts.estudioCohorteCSSFV.ftp.FTPConnection;
import ni.com.sts.estudioCohorteCSSFV.ftp.FTPParams;
import ni.com.sts.estudioCohorteCSSFV.modelo.ConsEstudios;
import ni.com.sts.estudioCohorteCSSFV.modelo.EscuelaCatalogo;
import ni.com.sts.estudioCohorteCSSFV.modelo.EstudioCatalogo;
import ni.com.sts.estudioCohorteCSSFV.modelo.HistEjecucionProcesoAutomatico;
import ni.com.sts.estudioCohorteCSSFV.modelo.Paciente;
import ni.com.sts.estudioCohorteCSSFV.servicios.ConsEstudiosService;
import ni.com.sts.estudioCohorteCSSFV.servicios.EscuelasCatalogoService;
import ni.com.sts.estudioCohorteCSSFV.servicios.EstudiosCatalogoService;
import ni.com.sts.estudioCohorteCSSFV.servicios.HistEjecucionProcesoService;
import ni.com.sts.estudioCohorteCSSFV.servicios.PacientesService;
import ni.com.sts.estudioCohorteCSSFV.servicios.ParametroService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.InfoResultado;
import ni.com.sts.estudioCohorteCSSFV.util.UtilDate;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

public class ActualizacionDatosThread extends Thread {
	private final Logger logger = Logger.getLogger(this.getClass());
	private CompositeConfiguration config;
	private ParametroService parametroService = new ParametrosDA();
	private EscuelasCatalogoService escuelasCatalogoService = new EscuelasCatalogoDA();
	private EstudiosCatalogoService estudiosCatalogoService = new EstudiosCatalogoDA();
	private PacientesService pacientesService = new PacientesDA();
	private ConsEstudiosService consEstudiosService = new ConsEstudiosDA();
	private HistEjecucionProcesoService histEjecucionProcesoService = new HistEjecucionProcesoDA();
	private Connection conn = null;
	private ConnectionDAO connectionDAO = new ConnectionDAO();
	
	private void abrirConexion(){
		conn = connectionDAO.getConection();
	}
	
	private void cerrarConexion() throws SQLException{
		if (conn!=null && !conn.isClosed()){
			conn.close();
			logger.info("Se cierra conexion");
		}
	}
	
	private void iniciarTransaccion() throws SQLException{
		conn.setAutoCommit(false);	
		logger.info("Se inicia transaccion");
	}
	
	private void confirmarTransaccion() throws SQLException{
		conn.commit();
		logger.info("Se confirma transaccion");
	}
	
	private void deshacerTransaccion() throws SQLException{
		conn.rollback();
		logger.info("Se deshace transaccion");
	}
	  
	@SuppressWarnings("static-access")
	public void run() {
		config = UtilProperty.getConfigurationfromExternalFile("estudioCohorteCSSFVAD.properties");
		UtilLog.setLog(config.getString("estudioCohorteCSSFVAD.log"));
		logger.info("Inicia "+this.getName());
		Connection connNoTransac = null;
		while(true){
			try{
				
				HistEjecucionProcesoAutomatico ejecucionProcesoHoy = histEjecucionProcesoService.getEjecucionProcesoFechaHoy("ACTDATOS");
				if (ejecucionProcesoHoy!=null){
					logger.debug("Se duerme main una hora");
					System.out.println("Se duerme main una hora");
					this.sleep(3600000);//3600000 si ya se ejecuto se duerme una hora, por si se cambia el parámetro de la hora ejecución se vuelva a ejecutar
					logger.debug("despierta main");
					System.out.println("despierta main");				
				}else{
					String valor = parametroService.getParametroByName("HORA_EJECUCION_ADPC");
					if (valor!=null){
						System.out.println("HORA_EJECUCION_CAOC = "+valor);
						Date dFechaHoy = new Date();
						String sFechaHoy = UtilDate.DateToString(dFechaHoy, "dd/MM/yyyy");
						Date dFechaEjecucion = UtilDate.StringToDate(sFechaHoy+" "+valor, "dd/MM/yyyy HH:mm");
						System.out.println(dFechaEjecucion.compareTo(dFechaHoy));
						if (dFechaEjecucion.compareTo(dFechaHoy) < 0){
							InfoResultado registroProceso = histEjecucionProcesoService.registrarEjecucionProceso("ACTDATOS");
							if (registroProceso.isOk()){
								connNoTransac = connectionDAO.getConection();
								abrirConexion();
							      //BD PDVI
								
								FTPParams params = new FTPParams();
								params.setServer(config.getString("ftp.server"));
								params.setPort(Integer.valueOf(config.getString("ftp.port")));
								params.setUser(config.getString("ftp.user"));
								params.setPass(config.getString("ftp.pass"));
								params.setFileName(config.getString("ftp.file.pdvi"));
								params.setRemoteFilePath(config.getString("ftp.remotePath.pdvi"));
								params.setLocalFilePath(config.getString("ftp.localPath"));				
								FTPConnection ftpClient = new FTPConnection();
								//InfoResultado resultado = ftpClient.downloadFile(params);
								//aca descargar archivo
								InfoResultado resultado = ftpClient.downloadFileSFTP(params);				
								System.out.println(resultado.getMensaje());
								logger.info(resultado.getMensaje());
								if (resultado.isOk()){							       
									cargarEscuelas(connNoTransac);
									cargarPacientes(connNoTransac);
									//eliminarlo
									File pdviFile = new File(params.getLocalFilePath()+params.getFileName());
									if(pdviFile.delete()){
										logger.info(params.getLocalFilePath()+params.getFileName()+ ":: eliminado");
									}else{
										logger.info(params.getLocalFilePath()+params.getFileName()+ ":: no fue eliminado");
									}
								}
								
								//BD ScanCartas
								params.setFileName(config.getString("ftp.file.scancartas"));
								params.setRemoteFilePath(config.getString("ftp.remotePath.scancartas"));
								//InfoResultado resultado = ftpClient.downloadFile(params);
								//aca descargar otro archivo
								resultado = ftpClient.downloadFileSFTP(params);
								System.out.println(resultado.getMensaje());
								logger.info(resultado.getMensaje());
								if (resultado.isOk()){
									cargarEstudios(connNoTransac);
									cargarConcentimientos(connNoTransac);
									//aca eliminarlo
									File pdviFile = new File(params.getLocalFilePath()+params.getFileName());
									if(pdviFile.delete()){
										logger.info(params.getLocalFilePath()+params.getFileName()+ ":: eliminado");
									}else{
										logger.info(params.getLocalFilePath()+params.getFileName()+ ":: no fue eliminado");
									}
								}
							}else{
								logger.error(registroProceso.getMensaje());
							}
						}else{
							logger.debug("Se duerme main 5 min");
							System.out.println("Se duerme main 5 min");
							Thread.currentThread().sleep(300000);//300000 si aún no es hora de ejecutar proceso se duerme 5 minutos
							logger.debug("despierta main");
							System.out.println("despierta main");
						}
					}else{
						logger.debug("Se duerme main 5 min. No se encontró valor de parámetro HORA_EJECUCION_ADPC");
						System.out.println("Se duerme main 5 min. No se encontró valor de parámetro HORA_EJECUCION_ADPC");
						this.sleep(300000);//300000 si aún no es hora de ejecutar proceso se duerme 5 minutos
						logger.debug("despierta main");
						System.out.println("despierta main");
					}
				}
				  
			}catch(Exception ex){
				logger.error("Error en la ejecucion de: "+this.getName(),ex);				
			}finally{
		    	logger.info("Proceso actualizacion datos terminado");
			      try {
			    		//rs.close();
			        	//stmt.close();
			    	  cerrarConexion();
			        if (connNoTransac!=null && !connNoTransac.isClosed()){
			        	connNoTransac.close();
			        	System.out.println("Conexión no transaccional cerrada");
			        }
			      } catch (Exception ee) {
			        ee.printStackTrace();
			      }
			      logger.info("Finaliza "+this.getName());
			}
		}

	}
	
	private void cargarEscuelas(Connection connNoTransac){
		  try { 
			  logger.info("Proceso actualizacion datos escuelas iniciado");
			  escuelasCatalogoService.setConnTransac(conn);
		      iniciarTransaccion();
		      escuelasCatalogoService.crearRespaldo(connNoTransac);
		      List<EscuelaCatalogo> escuelasList = escuelasCatalogoService.getEscuelasFromODBC();
		      if (escuelasList.size()>0){
			      for(EscuelaCatalogo escuela: escuelasList){
			    	  escuelasCatalogoService.AddEscuela(escuela);
			      }
			      escuelasCatalogoService.limpiarEscuelasTmp();
			      confirmarTransaccion();
		      }else {
		    	  logger.info("no se encontraron registros en base de datos ODBC, se procede a deshacerRespaldo ConsEstudiosDA");
		    	  try{
			    		escuelasCatalogoService.deshacerRespaldo(connNoTransac);		    		
			    		deshacerTransaccion();		    		
			    	}catch(Exception ex){
			    		ex.printStackTrace();
			    		logger.error("Error rollback transaction", ex);
			    	}  
		      }

		    } catch (Exception e) {
		      // handle the exception
		    	try{
		    		escuelasCatalogoService.deshacerRespaldo(connNoTransac);		    		
		    		deshacerTransaccion();		    		
		    	}catch(Exception ex){
		    		ex.printStackTrace();
		    		logger.error("Error rollback transaction", ex);
		    	}
		    	e.printStackTrace();
		    	System.err.println(e.getMessage());
		    	logger.error("Error cargarConcentimientos", e);
		    } finally {
		    	logger.info("Proceso actualizacion datos escuelas terminado");		      
		    }
	}

	private void cargarPacientes(Connection connNoTransac){
		try { 
			logger.info("Proceso actualizacion datos pacientes iniciado");
			  pacientesService.setConnTransac(conn);
		      iniciarTransaccion();
		      pacientesService.crearRespaldo(connNoTransac);
		      List<Paciente> pacientesList = pacientesService.getPacientesFromODBC();
		      if (pacientesList.size()>0){
			      for(Paciente paciente: pacientesList){
			    	  pacientesService.AddPaciente(paciente);
			      }
			      pacientesService.limpiarPacientesTmp();
			      confirmarTransaccion();
		      }else{
		    	  logger.info("no se encontraron registros en base de datos ODBC, se procede a deshacerRespaldo ConsEstudiosDA");
		    	  try{
			    		pacientesService.deshacerRespaldo(connNoTransac);		    		
			    		deshacerTransaccion();		    		
			    	}catch(Exception ex){
			    		ex.printStackTrace();
			    		logger.error("Error rollback transaction", ex);
			    	} 
		      }

		    } catch (Exception e) {
		      // handle the exception
		    	try{
		    		pacientesService.deshacerRespaldo(connNoTransac);		    		
		    		deshacerTransaccion();		    		
		    	}catch(Exception ex){
		    		ex.printStackTrace();
		    		logger.error("Error rollback transaction", ex);
		    	}
		    	e.printStackTrace();
		    	System.err.println(e.getMessage());
		    	logger.error("Error cargarConcentimientos", e);
		    } finally {
		    	logger.info("Proceso actualizacion datos pacientes terminado");		      
		    }
	}
	
	private void cargarEstudios(Connection connNoTransac){
		  try {
			  logger.info("Proceso actualizacion datos estudios iniciado");
			  estudiosCatalogoService.setConnTransac(conn);
		      iniciarTransaccion();
		      estudiosCatalogoService.crearRespaldo(connNoTransac);
		      List<EstudioCatalogo> estudiosList = estudiosCatalogoService.getEstudiosFromODBC();
		      if (estudiosList.size()>0){
			      for(EstudioCatalogo estudio: estudiosList){
			    	  estudiosCatalogoService.AddEstudio(estudio);
			      }
			      estudiosCatalogoService.limpiarEstudioTmp();
			      confirmarTransaccion();
		      }else {
		    	  logger.info("no se encontraron registros en base de datos ODBC, se procede a deshacerRespaldo ConsEstudiosDA");
		    	  try{
			    		estudiosCatalogoService.deshacerRespaldo(connNoTransac);		    		
			    		deshacerTransaccion();		    		
			    	}catch(Exception ex){
			    		ex.printStackTrace();
			    		logger.error("Error rollback transaction", ex);
			    	} 
		      }
		    } catch (Exception e) {
		      // handle the exception
		    	try{
		    		estudiosCatalogoService.deshacerRespaldo(connNoTransac);		    		
		    		deshacerTransaccion();		    		
		    	}catch(Exception ex){
		    		logger.error("Error rollback transaction", ex);
		    	}
		    	e.printStackTrace();
		    	System.err.println(e.getMessage());
		    	logger.error("Error cargarConcentimientos", e);
		    } finally {
		    	logger.info("Proceso actualizacion datos estudios terminado");		      
		    }
	}

	private void cargarConcentimientos(Connection connNoTransac){
		try { 
			logger.info("Proceso actualizacion datos consentimientos iniciado");
			  consEstudiosService.setConnTransac(conn);
		      iniciarTransaccion();
		      consEstudiosService.crearRespaldo(connNoTransac);
		      List<ConsEstudios> consEstudiosList = consEstudiosService.getConsEstudiosFromODBC();
		      if (consEstudiosList.size()>0){
			      for(ConsEstudios consEstudio: consEstudiosList){
			    	  consEstudiosService.AddConsEstudio(consEstudio);
			      }
			      consEstudiosService.limpiarConsEstudiosTmp();
			      confirmarTransaccion();
		      }else{
		    	  logger.info("no se encontraron registros en base de datos ODBC, se procede a deshacerRespaldo ConsEstudiosDA");
		    	  try{
			    		consEstudiosService.deshacerRespaldo(connNoTransac);		    		
			    		deshacerTransaccion();		    		
			    	}catch(Exception ex){
			    		ex.printStackTrace();
			    		logger.error("Error rollback transaction", ex);
			    	}
		      }

		    } catch (Exception e) {
		      // handle the exception
		    	try{
		    		consEstudiosService.deshacerRespaldo(connNoTransac);		    		
		    		deshacerTransaccion();		    		
		    	}catch(Exception ex){
		    		ex.printStackTrace();
		    		logger.error("Error rollback transaction", ex);
		    	}
		    	e.printStackTrace();
		    	System.err.println(e.getMessage());
		    	logger.error("Error cargarConcentimientos", e);
		    } finally {
		    	logger.info("Proceso actualizacion datos consentimientos terminado");		      
		    }
	}
}
