package ni.com.sts.estudioCohorteCSSFV.servicios;

import ni.com.sts.estudioCohorteCSSFV.modelo.HistEjecucionProcesoAutomatico;
import ni.com.sts.estudioCohorteCSSFV.util.InfoResultado;

public interface HistEjecucionProcesoService {

	public HistEjecucionProcesoAutomatico getEjecucionProcesoFechaHoy(String proceso);
	
	public InfoResultado registrarEjecucionProceso(String proceso);
}
