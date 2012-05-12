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

@XmlRootElement(name = "Tokken")
public class Tokken {
    
    private String usuario;
    private String token;
    private String fecha_creacion;
    private String ip;
    
    
  public String getUsuario() {
    return usuario;
  }

  @XmlElement
  public void setUsuario(String nombre) {
      System.out.println("Entro getUsuario"+nombre);
      this.usuario = nombre;
  }
  
  public String getTokken() {
    return token;
  }

  @XmlElement
  public void setTokken(String seguridad) {
    this.token = seguridad;
  }
  
  public String getFecha_creacion() {
    return fecha_creacion;
  }

  @XmlElement
  public void setFecha_creacion(String fecha) {
    this.fecha_creacion = fecha;
  }
  
  public String getIp() {
    return ip;
  }

  @XmlElement
  public void setIp(String dir) {
    this.ip = dir;
  }
    
}
