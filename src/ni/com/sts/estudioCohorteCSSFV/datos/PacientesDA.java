package ni.com.sts.estudioCohorteCSSFV.datos;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.naming.CannotProceedException;

import org.apache.axis.encoding.Base64;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import ni.com.sts.estudioCohorteCSSFV.modelo.Paciente;
import ni.com.sts.estudioCohorteCSSFV.servicios.PacientesService;
import ni.com.sts.estudioCohorteCSSFV.util.ConnectionDAO;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;
import ni.com.sts.estudioCohorteCSSFV.dto.ParticipanteDto;

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
            /*pst = connTransac.prepareStatement("INSERT INTO paciente(cod_expediente, nombre1, nombre2, apellido1, apellido2, sexo, fecha_nac, edad, " +
            		"estudiante, turno, escuela, tutor_nombre1, tutor_nombre2, tutor_apellido1, tutor_apellido2, direccion, telefono, telefono2, telefono3, retirado) " +
            		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");*/
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
            pst.setInt(8, 0);
            pst.setString(9, null);
            pst.setString(10, null);
            pst.setInt(11, 0);
            pst.setString(12, dato.getTutorNombre1());
            pst.setString(13, dato.getTutorNombre2());
            pst.setString(14, dato.getTutorApellido1());
            pst.setString(15, dato.getTutorApellido2());
            pst.setString(16, dato.getDireccion());
            pst.setString(17, null);
            pst.setString(18, null);
            pst.setString(19, null);
            pst.setString(20, String.valueOf(dato.getRetirado()));
            logger.info(dato.getCodExpediente());
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
	public List<Paciente> getPacientesFromServerBDEstudios() throws Exception {
		List<Paciente> pacientesList = new ArrayList<Paciente>();
		try {
			String PARTICIPANTES = config.getString("PARTICIPANTES");
			String user = config.getString("USER");
			String pwd = config.getString("PWD");
			URL url = new URL(PARTICIPANTES);
				
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
			
			 
			 List<ParticipanteDto> participanteDto = new ArrayList<ParticipanteDto>();
		     Type founderListType = founderListType = new TypeToken<ArrayList<ParticipanteDto>>(){}.getType();
		     participanteDto = new Gson().fromJson(response.toString(), founderListType);
		     for (int i = 0; i < participanteDto.size(); i++) {	
		    	 Paciente paciente = new Paciente();
		    	 paciente.setCodExpediente(participanteDto.get(i).getCodigo());
		    	 paciente.setNombre1(participanteDto.get(i).getNombre1());
		    	 if (participanteDto.get(i).getNombre2() != null && participanteDto.get(i).getNombre2() != "") {
		    		 paciente.setNombre2(participanteDto.get(i).getNombre2());
		    	 } else {
		    		 paciente.setNombre2("");
		    	 }
		    	 paciente.setApellido1(participanteDto.get(i).getApellido1());
		    	 if (participanteDto.get(i).getApellido2() != null & participanteDto.get(i).getApellido2() != "") {
		    		 paciente.setApellido2(participanteDto.get(i).getApellido2());
		    	 } else {
		    		 paciente.setApellido2("");
		    	 }
		    	 paciente.setSexo(participanteDto.get(i).getSexo().charAt(0));

		    	 long longDate = Long.valueOf(participanteDto.get(i).getFechaNac());
		    	 Date date = new Date(longDate);
		         		    	
		    	 paciente.setFechaNac(date);
		    	 
		    	 if (participanteDto.get(i).getDireccion() != null && participanteDto.get(i).getDireccion() != "") {
		    		 paciente.setDireccion(participanteDto.get(i).getDireccion());
		    	 } else {
		    		 paciente.setDireccion("");
		    	 }
		    	 
		    	 if (participanteDto.get(i).getNombre1Tutor() != null && participanteDto.get(i).getNombre1Tutor() != "") {
		    		 paciente.setTutorNombre1(participanteDto.get(i).getNombre1Tutor());
		    	 } else {
		    		 paciente.setTutorNombre1("");
		    	 }
		    	 
		    	 if (participanteDto.get(i).getNombre2Tutor() != null && participanteDto.get(i).getNombre2Tutor() != "") {
		    		 paciente.setTutorNombre2(participanteDto.get(i).getNombre2Tutor());
		    	 } else {
		    		 paciente.setTutorNombre2("");
		    	 }
		    	 
		    	 if (participanteDto.get(i).getApellido1Tutor() != null && participanteDto.get(i).getApellido1Tutor() != "") {
		    		 paciente.setTutorApellido1(participanteDto.get(i).getApellido1Tutor());
		    	 } else {
		    		 paciente.setTutorApellido1("");
		    	 }
		    	 
		    	 if (participanteDto.get(i).getApellido2Tutor() != null && participanteDto.get(i).getApellido2Tutor() != "") {
		    		 paciente.setTutorApellido2(participanteDto.get(i).getApellido2Tutor());
		    	 } else {
		    		 paciente.setTutorApellido2("");
		    	 }
		    	 paciente.setRetirado(participanteDto.get(i).getEstPart() <= 0 ? '1' : '0');
		    	 //paciente.setRetirado(rs.getBoolean("est_part")==false?'1':'0');
		    	 
		    	 pacientesList.add(paciente);
		     }
			 br.close();
			 http.disconnect();
		} catch (Throwable e) {
            e.printStackTrace();
            logger.error("Se ha producido un error al consultar base de datos ODBC :: PacientesDA" + "\n" + e.getMessage(),e);
            throw new Exception(e);
        }
		return pacientesList;
	}
	
	@Override
	public List<Paciente> getPacientesFromBDEstudios(Connection connNoTransacMySql) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		List<Paciente> pacientesList = new ArrayList<Paciente>();
        try {
        	String query = "select a.CODIGO, a.NOMBRE1, a.NOMBRE2, a.APELLIDO1, a.APELLIDO2, a.SEXO, a.FECHANAC, " + 
        			//"b.tutor, b.est_part, " +
        			" a.NOMBRE1_TUTOR, a.NOMBRE2_TUTOR, a.APELLIDO1_TUTOR, a.APELLIDO2_TUTOR, b.est_part, " +
        			"c.DIRECCION " +
        			"from participantes a " +
        			"inner join participantes_procesos b on a.CODIGO = b.codigo  " +
        			"inner join casas c on a.CODIGO_CASA = c.CODIGO";
        	stmt = connNoTransacMySql.createStatement();
        	rs = stmt.executeQuery(query);
		      while (rs.next()) {
		    	  Paciente paciente = new Paciente();
		    	  paciente.setCodExpediente(rs.getInt("CODIGO"));
		    	  
		    	  paciente.setNombre1(rs.getString("NOMBRE1"));
		    	  if (paciente.getNombre1() != null && paciente.getNombre1().length()>32)
		    		  paciente.setNombre1(paciente.getNombre1().substring(0,31));
		    	  
		    	  paciente.setNombre2(rs.getString("NOMBRE2"));
		    	  if (paciente.getNombre2() != null && paciente.getNombre2().length()>32)
		    		  paciente.setNombre2(paciente.getNombre2().substring(0,31));
		    	  
		    	  paciente.setApellido1(rs.getString("APELLIDO1"));
		    	  if (paciente.getApellido1() != null && paciente.getApellido1().length()>32)
		    		  paciente.setApellido1(paciente.getApellido1().substring(0,31));
		    	  
		    	  paciente.setApellido2(rs.getString("APELLIDO2"));
		    	  if (paciente.getApellido2() != null && paciente.getApellido2().length()>32)
		    		  paciente.setApellido2(paciente.getApellido2().substring(0,31));
		    	  
		    	  paciente.setSexo(rs.getString("SEXO").charAt(0));
		    	  paciente.setFechaNac(rs.getDate("FECHANAC"));
		    	  
		    	  paciente.setDireccion(rs.getString("DIRECCION"));
		    	  if (paciente.getDireccion() != null && paciente.getDireccion().length()>256)
		    		  paciente.setDireccion(paciente.getDireccion().substring(0,255));

		    	  paciente.setTutorNombre1(rs.getString("NOMBRE1_TUTOR"));
		    	  
		    	  paciente.setTutorNombre2(rs.getString("NOMBRE2_TUTOR"));
		    	  
		    	  paciente.setTutorApellido1(rs.getString("APELLIDO1_TUTOR"));
		    	  
		    	  paciente.setTutorApellido2(rs.getString("APELLIDO2_TUTOR"));
		    	  //saber cuantos espacios tiene
		    	  //si tiene un espacio es solo un nombre y un apellido
		    	 /* if (rs.getString("tutor")!=null && !rs.getString("tutor").trim().equals("")) {
		    	  int espacios = contarEspacios(rs.getString("tutor").trim());
			    	if (espacios == 0) {
			    		paciente.setTutorNombre1(rs.getString("tutor").trim().toUpperCase());
			    		paciente.setTutorApellido1("-");
			    	}else if (espacios == 1) {
			    		  String nombreTutor = rs.getString("tutor").trim();
			    		  String[] valores = nombreTutor.split(" ");
			    		  paciente.setTutorNombre1(valores[0]);
			    		  paciente.setTutorApellido1(valores[1]);
			    	  }else {
			    		  String nombreTutor = rs.getString("tutor").trim();
			    		  String[] valores = nombreTutor.split(" ");
			    		  if (valores.length>1) {
			    		  String parte1= valores[0]+ " "+valores[1];
			    		  String parte2 = valores[2];
			    		  String parte3="";
			    		  for (int i=3; i < valores.length;i++) {
			    			  parte2 += " "+valores[i];
			    		  }
			    		  if (parte2.length()>32) {
			    			  parte3= parte2.substring(parte2.lastIndexOf(" "), parte2.length());
			    			  parte2= parte2.substring(0,parte2.lastIndexOf(" "));
			    			  
			    		  }
			    		  paciente.setTutorNombre1(parte1.toUpperCase());
			    		  paciente.setTutorApellido1(parte2.trim().toUpperCase());
			    		  paciente.setTutorApellido2(parte3.trim().toUpperCase());
			    		  }else {
			    			  paciente.setTutorNombre1(nombreTutor);
			    			  paciente.setTutorApellido1("-");
			    		  }
			    	  }
		    	  }else {
		    		  paciente.setTutorNombre1("-");
		    		  paciente.setTutorApellido1("-");
		    	  }*/
		    	  //si tiene 
		    	  
		    	  /*paciente.setTutorNombre1(rs.getString("NOMBREPT"));
		    	  if (paciente.getTutorNombre1() != null && paciente.getTutorNombre1().length()>32)
		    		  paciente.setTutorNombre1(paciente.getTutorNombre1().substring(0,31));
		    	  
		    	  paciente.setTutorNombre2(rs.getString("NOMBREPT2"));
		    	  if (paciente.getTutorNombre2() != null && paciente.getTutorNombre2().length()>32)
		    		  paciente.setTutorNombre2(paciente.getTutorNombre2().substring(0,31));
		    	  
		    	  paciente.setTutorApellido1(rs.getString("APELLIDOPT"));
		    	  if (paciente.getTutorApellido1() != null && paciente.getTutorApellido1().length()>32)
		    		  paciente.setTutorApellido1(paciente.getTutorApellido1().substring(0,31));
		    	  
		    	  paciente.setTutorApellido2(rs.getString("APELLIDOPT2"));
		    	  if (paciente.getTutorApellido2() != null && paciente.getTutorApellido2().length()>32)
		    		  paciente.setTutorApellido2(paciente.getTutorApellido2().substring(0,31));*/
		    	  
		    	  paciente.setRetirado(rs.getBoolean("est_part")==false?'1':'0');
		    	  
		    	  
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
            } catch (SQLException ex) {
    			logger.error(" No se pudo cerrar conexión ", ex);
            }
        }
        return pacientesList;
	}

	private int contarEspacios(String cadena) {
		// El contador de espacios
        int cantidadDeEspacios = 0;
		for (int i = 0; i < cadena.length(); i++) {
            // Si el carácter en [i] es un espacio (' ') aumentamos el contador 
            if (cadena.charAt(i) == ' ') cantidadDeEspacios++;
        }
		return cantidadDeEspacios;
		
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
