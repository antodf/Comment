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

@XmlRootElement(name = "Etiqueta")
public class Etiqueta {
    
        String nombre;
        String commentario;
        String tokken;
    
  public String getNombre() {
    return nombre;
  }

  @XmlElement
  public void setNombre(String nombre) {
      this.nombre = nombre;
  }
  
  public String getCommentario() {
    return commentario;
  }

//  @XmlElement
  public void setCommentario(String nombre) {
      this.commentario = nombre;
  }
  
  public String getTokken() {
    return tokken;
  }

//  @XmlElement
  public void setTokken(String nombre) {
      this.tokken = nombre;
  }
    
}
