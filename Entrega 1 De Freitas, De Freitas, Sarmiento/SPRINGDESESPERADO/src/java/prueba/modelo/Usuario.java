/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.modelo;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 *
 * @author Antonio
 */

@XmlRootElement(name = "Usuario")
public class Usuario {
    
    private String nombre;
    private String apellido;
    private String clave;
    private String correo;
    private String nickname;
    private String fecha_nac;
    private String pais;
    private String biografia;
    private String foto;
    
  public String getNombre() {
    return nombre;
  }

  @XmlElement
  public void setNombre(String nombre) {
      this.nombre = nombre;
  }
  
  public String getApellido() {
    return apellido;
  }

  @XmlElement
  public void setApellido(String apellido) {
    this.apellido = apellido;
  }
  
  public String getClave() {
    return clave;
  }

  @XmlElement
  public void setClave(String clave) {
    this.clave = clave;
  }
  
  public String getCorreo() {
    return correo;
  }

  @XmlElement
  public void setCorreo(String correo) {
    this.correo = correo;
  }
  
  public String getNickname() {
    return nickname;
  }

  @XmlElement
  public void setNickname(String nickname) {
    this.nickname = nickname;
  }
  
  public String getFecha_nac() {
    return fecha_nac;
  }

  @XmlElement
  public void setFecha_nac(String fecha_nac) {
    this.fecha_nac = fecha_nac;
  }
  
  public String getPais() {
    return pais;
  }

  @XmlElement
  public void setPais(String pais) {
    this.pais = pais;
  }
  
  public String getBiografia() {
    return biografia;
  }

  @XmlElement
  public void setBiografia(String biografia) {
    this.biografia = biografia;
  }
  
  public String getFoto() {
    return foto;
  }

  @XmlElement
  public void setFoto(String foto) {
    this.foto = foto;
  }   
    
}
