package ni.com.sts.estudioCohorteCSSFV.servicios;

import java.sql.Connection;
import java.util.List;

import ni.com.sts.estudioCohorteCSSFV.modelo.ConsEstudios;

public interface ConsEstudiosService {

	public void AddConsEstudio(ConsEstudios dato) throws Exception;
	
	public void crearRespaldo(Connection connNoTransac) throws Exception;
	
	public void setConnTransac(Connection conn);
	
	public Connection getConnTransac();

	public void limpiarConsEstudiosTmp() throws Exception;

	public List<ConsEstudios> getConsEstudiosFromODBC() throws Exception;

	public void deshacerRespaldo(Connection connNoTransac) throws Exception;
	
}
