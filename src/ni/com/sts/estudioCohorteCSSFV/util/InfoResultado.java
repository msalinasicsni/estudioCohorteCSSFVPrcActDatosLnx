
package ni.com.sts.estudioCohorteCSSFV.util;

import java.util.List;

/**
 * Información del resultado de una transacción el cual se usará para progagar
 * dicho resultado a través de las diferentes capas de la aplicación.
 * que dicho resultado sea propagado a las diferentes capas de la aplicación.
 * <p>
 * @author STS
 * @version 1.0, &nbsp; 08/05/2013
 */

public class InfoResultado implements java.io.Serializable {

    private static final long serialVersionUID = 7301833333255795271L;

    /**
     * Nivel de gravedad del mensaje indicando un mensaje informativo en lugar de un error
     */
    public static final int INFO=1;
    /**
     * Nivel de gravedad del mensaje indicando que un error pudo haber ocurrido.
     */
    public static final int WARN=2;
    /**
     * Nivel de gravedad del mensaje indicando que se ha producido un error.
     */
    public static final int ERROR=3;
    /**
     * Nivel de gravedad del mensaje indicando que se ha producido un error muy grave.
     */
    public static final int FATAL=4;

    private Integer numeroError;
    private String  mensaje;
    private String  mensajeDetalle;
    private String  fuenteError;
    private Integer filasAfectadas;
    private boolean ok;
    private boolean excepcion;
    private int     gravedad;

    // Resultados
    private Object  objeto;
    @SuppressWarnings("rawtypes")
	private List    lista;
    private int     valor1;
    private int     valor2;
    private int     valor3;

    public InfoResultado() {

        this.numeroError=0;
        this.mensaje="";
        this.mensajeDetalle="";
        this.objeto=null;
        this.fuenteError="";
        this.ok=true;
        this.excepcion=false;
        this.gravedad=1;

    }

    /**
     * Establece el número de error, el cual puede ser una numeración propia
     * para su manipulación final o como número de referencia.
     */
    public void setNumeroError(Integer numeroError) {
        this.numeroError = numeroError;
    }

    /**
     * Obtiene el número de error, el cual puede ser una numeración propia para
     * su manipulación final o como número de referencia.
     */
    public Integer getNumeroError() {
        return numeroError;
    }

    /**
     * Establece un mensaje descriptivo que se generó.
     */
    public void setMensaje(String mensaje) {
        this.mensaje = mensaje;
    }

    /**
     * Obtiene el mensaje descriptivo que se generó.
     */
    public String getMensaje() {
        return mensaje;
    }

    /**
     * Encapsula como objeto el resultado propiamente dicho de la operación.
     * Debe ser <code>null</code> si se ha generado un error en dicha operación.
     */
    public void setObjeto(Object objeto) {
        this.objeto = objeto;
    }

    /**
     * Obtiene un objeto de tipo <code>Object</code> al cual debe aplicarse
     * el tipo correspondiente para su manipulación.
     */
    public Object getObjeto() {
        return objeto;
    }

    /**
     * Establece la fuente u origen donde se produjo el error, a fin de ser
     * un elemento orientativo para los errores graves o fatales.
     */
    public void setFuenteError(String fuenteError) {
        this.fuenteError = fuenteError;
    }

    /**
     * Obtiene la fuente u origen donde se produjo el error, a fin de ser
     * un elemento orientativo para los erroes graves o fatales.
     */
    public String getFuenteError() {
        return fuenteError;
    }

    /**
     * Establece el estado del resultado, indicando si se produjo o no un
     * error en la operación.  <br>
     * <code>true</code> si no se produjo error,
     * <code>false</code> si se ha producido un error.
     */
    public void setOk(boolean ok) {
        this.ok = ok;
    }

    /**
     * Obtiene el estado del resultado, indicando si se produjo o no un
     * error en la operación solicitada. <br>
     * Retorna <code>true</code> si no se produjo error,
     * <code>false</code> si se ha producido un error.
     */
    public boolean isOk() {
        return ok;
    }

    /**
     * Establece si el error que se produjo fue generado por
     * una excepción.<br>
     * <code>true</code> si no se produjo una excepción,
     * <code>false</code> si se ha producido una excepción.
     */
    public void setExcepcion(boolean excepcion) {
        this.excepcion = excepcion;
    }

    /**
     * Obtiene <code>true</code> si el error que se produjo fue generado por
     * una excepción o <code>false</code> caso contrario.<br>
     */
    public Boolean isExcepcion() {
        return excepcion;
    }

    /**
     * Establece un texto descriptivo con el detalle del error que se produjo.
     */
    public void setMensajeDetalle(String mensajeDetalle) {
        this.mensajeDetalle = mensajeDetalle;
    }

    /**
     * Obtiene un texto descriptivo con el detalle del error que se produjo.
     */
    public String getMensajeDetalle() {
        return mensajeDetalle;
    }

    /**
     * Establece la gravedad del error utilizando los mismos
     * valores de niveles de severidad del <code>FacesMessages</code><br>
     * 1=Info<br>
     * 2=Warning<br>
     * 3=Error<br>
     * 4=Fatal
     */
    public void setGravedad(int gravedad) {
        this.gravedad = gravedad;
    }

    /**
     * Obtiene la gravedad del error utilizando los mismos valores de
     * niveles de severidad del <code>FacesMessages</code><br>
     * 1=Info<br>
     * 2=Warning<br>
     * 3=Error<br>
     * 4=Fatal<p>
     * No es posible, con la versión de Mojarra (2.1.6), utilizar la
     * subclase Severity del FacesMessages, ya que esta es no serializable.
     */
    public int getGravedad() {
        return gravedad;
    }

    /**
     * Establece el número de filas afectadas en la transacción u operación
     * realizada y que dió lugar al objeto InfoResultado
     */
    public void setFilasAfectadas(Integer filasAfectadas) {
        this.filasAfectadas = filasAfectadas;
    }

    /**
     * Obtiene el número de filas afectadas en la transacción u operación
     * realizada y que dió lugar al objeto InfoResultado
     */
    public Integer getFilasAfectadas() {
        return filasAfectadas;
    }

    @SuppressWarnings("rawtypes")
	public List getLista() {
        return lista;
    }

    @SuppressWarnings("rawtypes")
	public void setLista(List lista) {
        this.lista = lista;
    }

    public int getValor1() {
        return valor1;
    }

    public void setValor1(int valor1) {
        this.valor1 = valor1;
    }

    public int getValor2() {
        return valor2;
    }

    public void setValor2(int valor2) {
        this.valor2 = valor2;
    }

    public int getValor3() {
        return valor3;
    }

    public void setValor3(int valor3) {
        this.valor3 = valor3;
    }

}
