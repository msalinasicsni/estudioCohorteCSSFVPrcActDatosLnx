package ni.com.sts.estudioCohorteCSSFV.servicios;

import java.sql.Connection;
import java.util.List;

import ni.com.sts.estudioCohorteCSSFV.modelo.EstudioCatalogo;

public interface EstudiosCatalogoService {
	
	public void AddEstudio(EstudioCatalogo dato) throws Exception;
	
	public void limpiarEstudios() throws Exception;
	
	public void crearRespaldo(Connection connNoTransac) throws Exception;

	public void setConnTransac(Connection conn);
	
	public Connection getConnTransac();

	public void limpiarEstudioTmp() throws Exception;

	public List<EstudioCatalogo> getEstudiosFromODBC() throws Exception;

	public void deshacerRespaldo(Connection connNoTransac) throws Exception;

	public List<EstudioCatalogo> getEstudiosFromDBEstudios(Connection connNoTransac) throws Exception;

}
