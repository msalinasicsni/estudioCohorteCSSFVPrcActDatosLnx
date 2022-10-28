package ni.com.sts.estudioCohorteCSSFV.dto;

import java.util.Date;

public class ConsentimientoDto {
	private String fecha;
    private Integer codigo;
    private Byte cons;
    private Byte asentChik;
    private Integer parteB;
    private Integer parteC;
    private Integer parteD;
    private Byte asentimientoEsc;
    private Integer parteE;
    private Integer parteF;
    private String tipoPartTrans;
    private String reactivacion;
    private String ahora;
    private boolean retirado;

    public String getFecha() {
        return fecha;
    }

    public void setFecha(String fecha) {
        this.fecha = fecha;
    }

    public Integer getCodigo() {
        return codigo;
    }

    public void setCodigo(Integer codigo) {
        this.codigo = codigo;
    }

    public Byte getCons() {
        return cons;
    }

    public void setCons(Byte cons) {
        this.cons = cons;
    }

    public Byte getAsentChik() {
        return asentChik;
    }

    public void setAsentChik(Byte asentChik) {
        this.asentChik = asentChik;
    }

    public Integer getParteB() {
        return parteB;
    }

    public void setParteB(Integer parteB) {
        this.parteB = parteB;
    }

    public Integer getParteC() {
        return parteC;
    }

    public void setParteC(Integer parteC) {
        this.parteC = parteC;
    }

    public Integer getParteD() {
        return parteD;
    }

    public void setParteD(Integer parteD) {
        this.parteD = parteD;
    }

    public Byte getAsentimientoEsc() {
        return asentimientoEsc;
    }

    public void setAsentimientoEsc(Byte asentimientoEsc) {
        this.asentimientoEsc = asentimientoEsc;
    }

    public Integer getParteE() {
        return parteE;
    }

    public void setParteE(Integer parteE) {
        this.parteE = parteE;
    }

    public Integer getParteF() {
        return parteF;
    }

    public void setParteF(Integer parteF) {
        this.parteF = parteF;
    }

    public String getTipoPartTrans() {
        return tipoPartTrans;
    }

    public void setTipoPartTrans(String tipoPartTrans) {
        this.tipoPartTrans = tipoPartTrans;
    }

    public String getReactivacion() {
        return reactivacion;
    }

    public void setReactivacion(String reactivacion) {
        this.reactivacion = reactivacion;
    }

    public String getAhora() {
        return ahora;
    }

    public void setAhora(String ahora) {
        this.ahora = ahora;
    }

    public boolean getRetirado() {
        return retirado;
    }

    public void setRetirado(boolean retirado) {
        this.retirado = retirado;
    }
}
