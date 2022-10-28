package ni.com.sts.estudioCohorteCSSFV.dto;

import java.util.Date;

public class ParticipanteDto {
	private Integer codigo;
    private String nombre1;
    private String nombre2;
    private String apellido1;
    private String apellido2;
    private String sexo;
    private String fechaNac;
    private String nombre1Tutor;
    private String nombre2Tutor;
    private String apellido1Tutor;
    private String apellido2Tutor;
    private String direccion;
    private Integer estPart;

    public Integer getCodigo() {
        return codigo;
    }

    public void setCodigo(Integer codigo) {
        this.codigo = codigo;
    }

    public String getNombre1() {
        return nombre1;
    }

    public void setNombre1(String nombre1) {
        this.nombre1 = nombre1;
    }

    public String getNombre2() {
        return nombre2;
    }

    public void setNombre2(String nombre2) {
        this.nombre2 = nombre2;
    }

    public String getApellido1() {
        return apellido1;
    }

    public void setApellido1(String apellido1) {
        this.apellido1 = apellido1;
    }

    public String getApellido2() {
        return apellido2;
    }

    public void setApellido2(String apellido2) {
        this.apellido2 = apellido2;
    }

    public String getSexo() {
        return sexo;
    }

    public void setSexo(String sexo) {
        this.sexo = sexo;
    }

    public String getFechaNac() {
        return fechaNac;
    }

    public void setFechaNac(String fechaNac) {
        this.fechaNac = fechaNac;
    }

    public String getNombre1Tutor() {
        return nombre1Tutor;
    }

    public void setNombre1Tutor(String nombre1Tutor) {
        this.nombre1Tutor = nombre1Tutor;
    }

    public String getNombre2Tutor() {
        return nombre2Tutor;
    }

    public void setNombre2Tutor(String nombre2Tutor) {
        this.nombre2Tutor = nombre2Tutor;
    }

    public String getApellido1Tutor() {
        return apellido1Tutor;
    }

    public void setApellido1Tutor(String apellido1Tutor) {
        this.apellido1Tutor = apellido1Tutor;
    }

    public String getApellido2Tutor() {
        return apellido2Tutor;
    }

    public void setApellido2Tutor(String apellido2Tutor) {
        this.apellido2Tutor = apellido2Tutor;
    }

    public String getDireccion() {
        return direccion;
    }

    public void setDireccion(String direccion) {
        this.direccion = direccion;
    }

    public Integer getEstPart() {
        return estPart;
    }

    public void setEstPart(Integer estPart) {
        this.estPart = estPart;
    }
}
