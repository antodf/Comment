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

@XmlRootElement(name = "Login")
public class usuarioLogin {
    
    private String nickname;
    private String clave;
    private String token;
    
    
    public String getNickname() {
    return nickname;
  }

  @XmlElement
  public void setNickname(String nombre) {
      this.nickname = nombre;
  }
  
  public String getClave() {
    return clave;
  }

  @XmlElement
  public void setClave(String pass) {
    this.clave = pass;
  }
  
  public String getToken() {
    return token;
  }

  @XmlElement
  public void setToken(String tok) {
    this.token = tok;
  }
    
}
