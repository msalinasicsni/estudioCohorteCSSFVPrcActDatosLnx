package ni.com.sts.estudioCohorteCSSFV.servicios;

import java.sql.Connection;
import java.util.List;

import ni.com.sts.estudioCohorteCSSFV.modelo.EscuelaCatalogo;

public interface EscuelasCatalogoService {
	
	public void AddEscuela(EscuelaCatalogo dato) throws Exception;
	
	public void limpiarEscuelas() throws Exception;
	
	public void crearRespaldo(Connection connNoTransac) throws Exception;
	
	public void setConnTransac(Connection conn);
	
	public Connection getConnTransac();

	public void limpiarEscuelasTmp() throws Exception;

	public List<EscuelaCatalogo> getEscuelasFromODBC() throws Exception;

	public void deshacerRespaldo(Connection connNoTransac) throws Exception;

	public List<EscuelaCatalogo> getEscuelasFromBDEstudios(Connection connNoTransac) throws Exception;
	
	public List<EscuelaCatalogo> getEscuelasFromServerBDEstudios() throws Exception;
	
}
