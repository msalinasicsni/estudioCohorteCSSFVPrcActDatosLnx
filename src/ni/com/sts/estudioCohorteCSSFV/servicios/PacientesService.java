package ni.com.sts.estudioCohorteCSSFV.servicios;

import java.sql.Connection;
import java.util.List;

import ni.com.sts.estudioCohorteCSSFV.modelo.Paciente;

public interface PacientesService {

	public void AddPaciente(Paciente dato) throws Exception;
	
	public void crearRespaldo(Connection connNoTransac) throws Exception;
	
	public void setConnTransac(Connection conn);
	
	public Connection getConnTransac();

	public void limpiarPacientesTmp() throws Exception;

	public List<Paciente> getPacientesFromODBC() throws Exception;

	public void deshacerRespaldo(Connection connNoTransac) throws Exception;
	
}
