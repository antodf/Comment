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

@XmlRootElement(name = "author")
public class author {
    
    private String name;
    private String password;
    private String twitter;
    private String correo;
    
  public String getName() {
    return name;
  }

  @XmlElement
  public void setName(String name) {
    this.name = name;
  }  
    
  public String getPassword() {
    return password;
  }

  @XmlElement
  public void setPassword(String password) {
    this.password = password;
  }
  
  public String getTwitter() {
    return twitter;
  }

  @XmlElement
  public void setTwitter(String twitter) {
    this.twitter = twitter;
  }
  
  public String getCorreo() {
    return correo;
  }

  @XmlElement
  public void setCorreo(String correo) {
    this.correo = correo;
  }
  
    @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}
