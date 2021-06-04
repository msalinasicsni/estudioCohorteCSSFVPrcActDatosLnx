package ni.com.sts.estudioCohorteCSSFV.thread;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import ni.com.sts.estudioCohorteCSSFV.datos.ConsEstudiosDA;
import ni.com.sts.estudioCohorteCSSFV.datos.EscuelasCatalogoDA;
import ni.com.sts.estudioCohorteCSSFV.datos.EstudiosCatalogoDA;
import ni.com.sts.estudioCohorteCSSFV.datos.HistEjecucionProcesoDA;
import ni.com.sts.estudioCohorteCSSFV.datos.PacientesDA;
import ni.com.sts.estudioCohorteCSSFV.datos.ParametrosDA;
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
		System.out.println(getFechaHoraActual()+" "+"INICIO");
		Connection connNoTransac = null;
		Connection connNoTransacMySql = null;
		while(true){
			try{
				
				HistEjecucionProcesoAutomatico ejecucionProcesoHoy = histEjecucionProcesoService.getEjecucionProcesoFechaHoy("ACTDATOS");
				if (ejecucionProcesoHoy != null) {
					logger.debug("Se duerme main una hora");
					System.out.println(getFechaHoraActual()+" "+"Se duerme main una hora");
					this.sleep(3600000);// 3600000 si ya se ejecuto se duerme una hora, por si se cambia el parámetro de
										// la hora ejecución se vuelva a ejecutar
					logger.debug("despierta main");
					System.out.println(getFechaHoraActual()+" "+"despierta main");
				} else {
					String valor = parametroService.getParametroByName("HORA_EJECUCION_ADPC");
					if (valor != null) {
						System.out.println(getFechaHoraActual()+" "+"HORA_EJECUCION_CAOC = " + valor);
						Date dFechaHoy = new Date();
						String sFechaHoy = UtilDate.DateToString(dFechaHoy, "dd/MM/yyyy");
						Date dFechaEjecucion = UtilDate.StringToDate(sFechaHoy+" "+valor, "dd/MM/yyyy HH:mm");
						System.out.println(getFechaHoraActual()+" "+dFechaEjecucion.compareTo(dFechaHoy));
						if (dFechaEjecucion.compareTo(dFechaHoy) < 0) {
							InfoResultado registroProceso = histEjecucionProcesoService.registrarEjecucionProceso("ACTDATOS");
							if (registroProceso.isOk()) {
								connNoTransac = connectionDAO.getConection();
								connNoTransacMySql = connectionDAO.getConectionMySql();
								abrirConexion();
								cargarEscuelas(connNoTransac, connNoTransacMySql);
								cargarEstudios(connNoTransac, connNoTransacMySql);
								cargarPacientes(connNoTransac, connNoTransacMySql);
								cargarConcentimientos(connNoTransac, connNoTransacMySql);
							} else {
								logger.debug("Se duerme main 5 min");
								System.out.println(getFechaHoraActual()+" "+"Se duerme main 5 min");
								Thread.currentThread().sleep(300000);// 300000 si aún no es hora de ejecutar proceso se
																		// duerme 5 minutos
								logger.debug("despierta main");
								System.out.println(getFechaHoraActual()+" "+"despierta main");
							}
						} else {
							logger.debug("Se duerme main 5 min. No se encontró valor de parámetro HORA_EJECUCION_ADPC");
							System.out.println(getFechaHoraActual()+" "+"Se duerme main 5 min. No se encontró valor de parámetro HORA_EJECUCION_ADPC");
							this.sleep(300000);// 300000 si aún no es hora de ejecutar proceso se duerme 5 minutos
							logger.debug("despierta main");
							System.out.println(getFechaHoraActual()+" "+"despierta main");
						}
					}
				}
			}catch(Exception ex){
				logger.error("Error en la ejecucion de: "+this.getName(),ex);
			}finally{
		    	logger.info("Proceso actualizacion datos terminado");
			      try {
			    	  cerrarConexion();
			        if (connNoTransac!=null && !connNoTransac.isClosed()){
			        	connNoTransac.close();
			        	logger.debug("Conexión no transaccional postgresql cerrada");
			        	System.out.println(getFechaHoraActual()+" "+"Conexión no transaccional postgresql cerrada");
			        }
			        if (connNoTransacMySql!=null && !connNoTransacMySql.isClosed()){
			        	connNoTransacMySql.close();
			        	logger.debug("Conexión no transaccional mysql cerrada");
			        	System.out.println(getFechaHoraActual()+" "+"Conexión no transaccional mysql cerrada");
			        }
			      } catch (Exception ee) {
			        ee.printStackTrace();
			      }
			      logger.info("Finaliza "+this.getName());
			      System.out.println(getFechaHoraActual()+" "+"FINALIZA");
			}
		}
	}
	
	private void cargarEscuelas(Connection connNoTransac, Connection connNoTransacMySql){
		  try { 
			  logger.info("Proceso actualizacion datos escuelas iniciado");
			  escuelasCatalogoService.setConnTransac(conn);
		      iniciarTransaccion();
		      escuelasCatalogoService.crearRespaldo(connNoTransac);
		      List<EscuelaCatalogo> escuelasList = escuelasCatalogoService.getEscuelasFromBDEstudios(connNoTransacMySql);
		      if (escuelasList.size()>0){
			      for(EscuelaCatalogo escuela: escuelasList){
			    	  escuelasCatalogoService.AddEscuela(escuela);
			      }
			      escuelasCatalogoService.limpiarEscuelasTmp();
			      confirmarTransaccion();
		      }else {
		    	  logger.info("no se encontraron registros en base de datos MySql estudios, se procede a deshacerRespaldo ConsEstudiosDA");
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

	private void cargarPacientes(Connection connNoTransac, Connection connNoTransacMySql){
		try { 
			logger.info("Proceso actualizacion datos pacientes iniciado");
			  pacientesService.setConnTransac(conn);
		      iniciarTransaccion();
		      pacientesService.crearRespaldo(connNoTransac);
		      List<Paciente> pacientesList = pacientesService.getPacientesFromBDEstudios(connNoTransacMySql);
		      if (pacientesList.size()>0){
			      for(Paciente paciente: pacientesList){
			    	  pacientesService.AddPaciente(paciente);
			      }
			      pacientesService.limpiarPacientesTmp();
			      confirmarTransaccion();
		      }else{
		    	  logger.info("no se encontraron registros en base de datos MySql estudios, se procede a deshacerRespaldo ConsEstudiosDA");
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
	
	private void cargarEstudios(Connection connNoTransac, Connection connNoTransacMySql){
		  try {
			  logger.info("Proceso actualizacion datos estudios iniciado");
			  estudiosCatalogoService.setConnTransac(conn);
		      iniciarTransaccion();
		      estudiosCatalogoService.crearRespaldo(connNoTransac);
		      List<EstudioCatalogo> estudiosList = estudiosCatalogoService.getEstudiosFromDBEstudios(connNoTransacMySql);
		      if (estudiosList.size()>0){
			      for(EstudioCatalogo estudio: estudiosList){
			    	  estudiosCatalogoService.AddEstudio(estudio);
			      }
			      estudiosCatalogoService.limpiarEstudioTmp();
			      confirmarTransaccion();
		      }else {
		    	  logger.info("no se encontraron registros en base de datos MySql estudios, se procede a deshacerRespaldo ConsEstudiosDA");
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

	private void cargarConcentimientos(Connection connNoTransac, Connection connNoTransacMySql) {
		try {
			logger.info("Proceso actualizacion datos consentimientos iniciado");
			consEstudiosService.setConnTransac(conn);
			iniciarTransaccion();
			consEstudiosService.crearRespaldo(connNoTransac);
			List<ConsEstudios> consEstudiosList = consEstudiosService.getConsEstudiosFromBDEstudios(connNoTransacMySql);
			if (consEstudiosList.size() > 0) {
				for (ConsEstudios consEstudio : consEstudiosList) {
					consEstudiosService.AddConsEstudio(consEstudio);
				}
				consEstudiosService.limpiarConsEstudiosTmp();
				confirmarTransaccion();
			} else {
				logger.info(
						"no se encontraron registros en base de datos MySql estudios, se procede a deshacerRespaldo ConsEstudiosDA");
				try {
					consEstudiosService.deshacerRespaldo(connNoTransac);
					deshacerTransaccion();
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error("Error rollback transaction", ex);
				}
			}

		} catch (Exception e) {
			// handle the exception
			try {
				consEstudiosService.deshacerRespaldo(connNoTransac);
				deshacerTransaccion();
			} catch (Exception ex) {
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
	
	public String getFechaHoraActual() {
		String fecha = null;
		try {
			DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
			Date date = new Date();
			
			fecha = dateFormat.format(date);
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		return fecha;
	}
}
