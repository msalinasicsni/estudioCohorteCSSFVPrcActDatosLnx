import ni.com.sts.estudioCohorteCSSFV.thread.ActualizacionDatosThread;

public class CargaAutomatica {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		
			ejecutarProceso();
		

	}
	
	public static void ejecutarProceso(){
		ActualizacionDatosThread carga = new ActualizacionDatosThread();
		carga.setName("ProcesoActualizacionAutomaticaDatos");
		carga.start();
	}

}
