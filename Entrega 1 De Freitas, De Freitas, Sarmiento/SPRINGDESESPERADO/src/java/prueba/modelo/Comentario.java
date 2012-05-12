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

@XmlRootElement(name = "Comentario")
public class Comentario {
    
    private String id;
    private String mensaje;
    private String fecha_creacion;
    private String adjunto;
    private String nickName;
    private String reply;

  public String getId() {
    return id;
  }

  @XmlElement
  public void setId(String id) {
    this.id = id;
  }    
    
  public String getmensaje() {
    return mensaje;
  }

  @XmlElement
  public void setmensaje(String men) {
    this.mensaje = men;
  }
  
  public String getFecha_creacion() {
    return fecha_creacion;
  }

  @XmlElement
  public void setFecha_creacion(String fecha_creacion) {
    this.fecha_creacion = fecha_creacion;
  }
  
  public String getAdjunto() {
    return adjunto;
  }

  @XmlElement
  public void setAdjunto(String adjunto) {
    this.adjunto = adjunto;
  }
  
    public String getnickName() {
    return nickName;
  }

//  @XmlElement
  public void setnickName(String nickName) {
    this.nickName = nickName;
  }
  
  public String getReply() {
    return reply;
  }

//  @XmlElement
  public void setReply(String re) {
    this.reply = re;
  }
    
}
