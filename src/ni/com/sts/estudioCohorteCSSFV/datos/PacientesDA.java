package ni.com.sts.estudioCohorteCSSFV.datos;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

import ni.com.sts.estudioCohorteCSSFV.modelo.Paciente;
import ni.com.sts.estudioCohorteCSSFV.servicios.PacientesService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;

public class PacientesDA extends ConnectionDAO implements PacientesService {

	private final Logger logger = Logger.getLogger(this.getClass());
	private Connection connTransac=null;
	private CompositeConfiguration config;
	
	public PacientesDA(){
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
	public void AddPaciente(Paciente dato) throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("INSERT INTO paciente(cod_expediente, nombre1, nombre2, apellido1, apellido2, sexo, fecha_nac, edad, " +
            		"estudiante, turno, escuela, tutor_nombre1, tutor_nombre2, tutor_apellido1, tutor_apellido2, direccion, telefono, telefono2, telefono3, retirado) " +
            		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            pst.setInt(1, dato.getCodExpediente());
            pst.setString(2, dato.getNombre1());
            pst.setString(3, dato.getNombre2());
            pst.setString(4, dato.getApellido1());
            pst.setString(5, dato.getApellido2());
            pst.setString(6, String.valueOf(dato.getSexo()));
            pst.setDate(7, (Date)dato.getFechaNac());
            pst.setInt(8, dato.getEdad());
            pst.setString(9, String.valueOf(dato.getEstudiante()));
            pst.setString(10, String.valueOf(dato.getTurno()));
            pst.setInt(11, dato.getEscuela());
            pst.setString(12, dato.getTutorNombre1());
            pst.setString(13, dato.getTutorNombre2());
            pst.setString(14, dato.getTutorApellido1());
            pst.setString(15, dato.getTutorApellido2());
            pst.setString(16, dato.getDireccion());
            pst.setString(17, dato.getTelefono());
            pst.setString(18, dato.getTelefono2());
            pst.setString(19, dato.getTelefono3());
            pst.setString(20, String.valueOf(dato.getRetirado()));
            
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al guardar el registro :: PacientesDA" + "\n" + e.getMessage(),e);
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
            st.addBatch("DELETE FROM paciente_tmp");
            st.addBatch("INSERT INTO paciente_tmp(cod_expediente, nombre1, nombre2, apellido1, apellido2, sexo, fecha_nac, edad, estudiante, turno, escuela, tutor_nombre1, tutor_nombre2, tutor_apellido1, tutor_apellido2, direccion, telefono, telefono2, telefono3, retirado) " +
            		"SELECT cod_expediente, nombre1, nombre2, apellido1, apellido2, sexo, fecha_nac, edad, estudiante, turno, escuela, tutor_nombre1, tutor_nombre2, tutor_apellido1, tutor_apellido2, direccion, telefono, telefono2, telefono3, retirado FROM paciente");
            st.addBatch("DELETE FROM paciente");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al crear respaldo :: PacientesDA" + "\n" + e.getMessage(),e);
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
	public void limpiarPacientesTmp() throws Exception {
		PreparedStatement pst = null;
        try {
            pst = connTransac.prepareStatement("DELETE FROM paciente_tmp");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al eliminar temporal pacientes :: PacientesDA" + "\n" + e.getMessage(),e);
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
	public List<Paciente> getPacientesFromODBC() throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		Connection connODBC = null;
		List<Paciente> pacientesList = new ArrayList<Paciente>();
        try {
        	//connODBC = getODBCConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.pdvi")),config.getString("dns.bdpdvi.password"),"");//getODBCConnection(config.getString("dns.bdpdvi"));
        	connODBC = getUcanaccessCryptedConnection(config.getString("ftp.localPath").concat(config.getString("ftp.file.pdvi")),config.getString("dns.bdpdvi.password"),"");
        	String query = "select codigo, nombre, nombre2, apellido, apellido2, sexo, fechana, edad, estud, turno, escuela, nombrept, nombrept2, " +
		      		"apellidopt, apellidopt2, direc, Telefono1, Telefono2, Telefono3, retirado  " +
		      		"from Datos_Generales_Data";
        	stmt = connODBC.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  Paciente paciente = new Paciente();
		    	  paciente.setCodExpediente(rs.getInt("codigo"));
		    	  
		    	  paciente.setNombre1(rs.getString("nombre"));
		    	  if (paciente.getNombre1() != null && paciente.getNombre1().length()>32)
		    		  paciente.setNombre1(paciente.getNombre1().substring(0,31));
		    	  
		    	  paciente.setNombre2(rs.getString("nombre2"));
		    	  if (paciente.getNombre2() != null && paciente.getNombre2().length()>32)
		    		  paciente.setNombre2(paciente.getNombre2().substring(0,31));
		    	  
		    	  paciente.setApellido1(rs.getString("apellido"));
		    	  if (paciente.getApellido1() != null && paciente.getApellido1().length()>32)
		    		  paciente.setApellido1(paciente.getApellido1().substring(0,31));
		    	  
		    	  paciente.setApellido2(rs.getString("apellido2"));
		    	  if (paciente.getApellido2() != null && paciente.getApellido2().length()>32)
		    		  paciente.setApellido2(paciente.getApellido2().substring(0,31));
		    	  
		    	  paciente.setSexo(rs.getString("sexo").charAt(0));
		    	  paciente.setFechaNac(rs.getDate("fechana"));
		    	  paciente.setEdad(rs.getShort("edad"));
		    	  paciente.setEstudiante(rs.getString("estud").charAt(0));
		    	  paciente.setTurno(rs.getString("turno").charAt(0));
		    	  paciente.setEscuela(rs.getShort("escuela"));
		    	  
		    	  paciente.setTutorNombre1(rs.getString("nombrept"));
		    	  if (paciente.getTutorNombre1() != null && paciente.getTutorNombre1().length()>32)
		    		  paciente.setTutorNombre1(paciente.getTutorNombre1().substring(0,31));
		    	  
		    	  paciente.setTutorNombre2(rs.getString("nombrept2"));
		    	  if (paciente.getTutorNombre2() != null && paciente.getTutorNombre2().length()>32)
		    		  paciente.setTutorNombre2(paciente.getTutorNombre2().substring(0,31));
		    	  
		    	  paciente.setTutorApellido1(rs.getString("apellidopt"));
		    	  if (paciente.getTutorApellido1() != null && paciente.getTutorApellido1().length()>32)
		    		  paciente.setTutorApellido1(paciente.getTutorApellido1().substring(0,31));
		    	  
		    	  paciente.setTutorApellido2(rs.getString("apellidopt2"));
		    	  if (paciente.getTutorApellido2() != null && paciente.getTutorApellido2().length()>32)
		    		  paciente.setTutorApellido2(paciente.getTutorApellido2().substring(0,31));
		    	  
		    	  paciente.setDireccion(rs.getString("direc"));
		    	  if (paciente.getDireccion() != null && paciente.getDireccion().length()>256)
		    		  paciente.setDireccion(paciente.getDireccion().substring(0,255));
		    	  
		    	  paciente.setTelefono(rs.getString("Telefono1"));
		    	  if (paciente.getTelefono() != null && paciente.getTelefono().length()>32)
		    		  paciente.setTelefono(paciente.getTelefono().substring(0,31));
		    	  
		    	  paciente.setTelefono2(rs.getString("Telefono2"));
		    	  if (paciente.getTelefono2() != null && paciente.getTelefono2().length()>32)
		    		  paciente.setTelefono2(paciente.getTelefono2().substring(0,31));
		    	  
		    	  paciente.setTelefono3(rs.getString("Telefono3"));
		    	  if (paciente.getTelefono3() != null && paciente.getTelefono3().length()>32)
		    		  paciente.setTelefono3(paciente.getTelefono3().substring(0,31));
		    	  
		    	  paciente.setRetirado(rs.getBoolean("retirado")==true?'1':'0');
		    	  
		    	  //logger.info(paciente.getCodExpediente() + " " + paciente.getNombre1()+ " " + paciente.getNombre2()+ " " + paciente.getApellido1()+ " " + paciente.getApellido2());		    	  
		    	  pacientesList.add(paciente);
		      }
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: PacientesDA" + "\n" + e.getMessage(),e);
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
        return pacientesList;
	}

	@Override
	public void deshacerRespaldo(Connection connNoTransac) throws Exception {
		Statement st = null;
        try {
        	logger.info("deshacerRespaldo PacientesDA");
            st = connNoTransac.createStatement();
            st.addBatch("DELETE FROM paciente");
            st.addBatch("INSERT INTO paciente(cod_expediente, nombre1, nombre2, apellido1, apellido2, sexo, fecha_nac, edad, estudiante, turno, escuela, tutor_nombre1, tutor_nombre2, tutor_apellido1, tutor_apellido2, direccion, telefono, telefono2, telefono3, retirado) " +
            		"SELECT cod_expediente, nombre1, nombre2, apellido1, apellido2, sexo, fecha_nac, edad, estudiante, turno, escuela, tutor_nombre1, tutor_nombre2, tutor_apellido1, tutor_apellido2, direccion, telefono, telefono2, telefono3, retirado " +
            		"FROM paciente_tmp");
            //st.addBatch("DELETE FROM paciente_tmp");
            int counts[] = st.executeBatch();
            System.out.println("Committed " + counts.length + " updates");
            logger.info("Committed " + counts.length + " updates");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al crear respaldo :: PacientesDA" + "\n" + e.getMessage(),e);
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
