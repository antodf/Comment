/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.modelo;

import java.io.FileInputStream;
import java.util.Properties;

import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;


/**
 * @author JAAC
 *
 */
public class EnviarCorreo {

//final static String CONFIG_FILE = "mail.props";
static String servidorcorreo;
static String correorigen;
static String propiedad;
static String puerto;
static String clave;

    public EnviarCorreo() 
    {
        servidorcorreo = "smtp.gmail.com";
        correorigen = "desarrollo.mail.spring@gmail.com";
        propiedad = "true";
        puerto = "587";
        clave = "desarrollo1234";
    }

    
public void enviarEmail( String sAsunto, String sTexto, String cDestino)
{
    try
    {
       Properties props = new Properties();

        // Nombre del host de correo, es smtp.gmail.com
        props.setProperty("mail.smtp.host", servidorcorreo);

        // TLS si está disponible
        props.setProperty("mail.smtp.starttls.enable", propiedad);

        // Puerto de gmail para envio de correos
        props.setProperty("mail.smtp.port",puerto);

        // Nombre del usuario
        props.setProperty("mail.smtp.user", correorigen);

        // Si requiere o no usuario y password para conectarse.
        props.setProperty("mail.smtp.auth", propiedad); 
        
        Session session = Session.getDefaultInstance(props);
        
        MimeMessage message = new MimeMessage(session);
        
        // Quien envia el correo
        message.setFrom(new InternetAddress(correorigen));

        // A quien va dirigido
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(cDestino));

        message.setSubject(sAsunto);
        
        message.setText(sTexto);

        Transport t = session.getTransport("smtp");
        
        t.connect(correorigen,clave);

        t.sendMessage(message,message.getAllRecipients());

        t.close();

  }
    catch (Exception e)
    {
        System.out.println("Error al enviar correo");
    }
}
    

}