/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;
import com.sun.xml.fastinfoset.stax.events.Util;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import prueba.DAO.*;
import prueba.modelo.Comentario;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;

/**
 *
 * @author Dans
 */

@Controller
public class insertarAdjunto extends HttpServlet {
    
    public static final String DIR_REPOSITORIO = "C:/Users/Antonio/Dropbox/Desarrollo/Adjuntos";
    
    private comentarioDAO dao;
    private usuarioDAO daoUser;
    private tokkenDAO daoTok;
    private String arreglo[];
    private String diasss[];
    private String horasss[];
    private Calendar calendario;
    String horas, minutos;
    int month;
    String dia, mes, anno, fechaCon;

    //private XStream stream = new XStream();
 
    @RequestMapping(value = "/insertarAdjunto", method = RequestMethod.POST,consumes = "multipart/form-data")
        
    	public @ResponseBody Comentario insertarAdjuntoInXML(HttpServletRequest request,String contenido,String nickName, String token)
            throws ServletException, IOException {
             boolean isMultipart = ServletFileUpload.isMultipartContent(request);
             System.out.println("string adjunto" + contenido +" : "+ nickName +":" +token);
                System.out.println("hola InsertarAdjunto");
                
                Comentario comment = new Comentario();
                
                dao = new comentarioDAOImpl();
                daoUser = new usuarioDAOImpl();
                
                if (Util.isEmptyString(contenido)){
                comment.setmensaje("ERROR 013: Contenido Nulo");
                comment.setnickName("ERROR 013");
                 return comment;
                
               } 
                 else
                  
                    
                     if (Util.isEmptyString(nickName)) {
                comment.setmensaje("ERROR 014");
                comment.setnickName("ERROR 014: NickName nulo");
                 return comment;
                
               } 
                 else
                  
                    
                     if (Util.isEmptyString(token)) {
                comment.setmensaje("ERROR 015");
                comment.setnickName("ERROR 015: Token nulo");
                 return comment;
                
               } 
                System.out.println(request.getContentLength());
                     if (request.getContentLength()<= 50) {
                 comment.setmensaje("ERROR 016: El request recibido no contiene ningun archivo");
                comment.setnickName("ERROR 016");
                 return comment;
               } 
//                comment.setmensaje(contenido);
//                comment.setnickName(nickName);
                     
              Usuario existe = new Usuario();
               
               existe = daoUser.findUsuario(nickName);
                
               Tokken findTokenViejo = new Tokken();
               
               daoTok = new tokkenDAOImpl();
               
               findTokenViejo = daoTok.findTokken(nickName);
               
               if(!existe.getNickname().isEmpty()){
               
               if ((!findTokenViejo.getTokken().isEmpty())&&(findTokenViejo.getTokken().equals(token))) {
                    
                   if(findTokenViejo.getUsuario().equals(nickName)) {
                   
                    calendario = Calendar.getInstance();
                
                    horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
                    minutos = Integer.toString(calendario.get(Calendar.MINUTE));
                    dia = Integer.toString(calendario.get(Calendar.DATE));
                    month = calendario.get(Calendar.MONTH);
                    month = month + 1;
                    mes = Integer.toString(month);
                    anno = Integer.toString(calendario.get(Calendar.YEAR));
                
                    fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
                    
                    String fechaVieja = findTokenViejo.getFecha_creacion();
                    
                    arreglo = fechaVieja.split(",");
                    String days = arreglo[0];
                    String hours = arreglo[1];
                    
                    diasss = days.split("/");
                    String diaViejo = diasss[0];
                    String mesViejo = diasss[1];
                    String annoViejo = diasss[2];
                    
                    horasss = hours.split(":");
                    String horaViejo = horasss[0];
                    String minutoViejo = horasss[1];
                    
                    if ((dia.equals(diaViejo)) && (mes.equals(mesViejo)) && (anno.equals(annoViejo))) {
                        
                        int newHora = Integer.parseInt(horas);
                        int oldHora = Integer.parseInt(horaViejo);
                        
                        int oldMinutos = Integer.parseInt(minutoViejo);
                        
                        newHora = newHora*60;
                        oldHora = oldHora*60;
                        
                        newHora = newHora + calendario.get(Calendar.MINUTE);
                        oldHora = oldHora + oldMinutos;
                        
                        int total = newHora - oldHora;
                        
                        if (total <= 3){   //AQUI CAMBIAR PARA LA DURACION DE LOS MINUTOS
                            
                            dao = new comentarioDAOImpl();

                            String idD = dao.getID();
                            System.out.println("ID mas grande de la base de datos: "+idD);

                            int codigoID;
                            codigoID = Integer.parseInt(idD);
                            codigoID = codigoID + 1;

                            idD = String.valueOf(codigoID);
                            System.out.println("ID a enviar al dao: "+idD);
                            comment.setId(idD);
                            comment.setReply("0");
                            comment.setAdjunto("Si");

                            calendario = Calendar.getInstance();

                            horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
                            minutos = Integer.toString(calendario.get(Calendar.MINUTE));
                            dia = Integer.toString(calendario.get(Calendar.DATE));
                            month = calendario.get(Calendar.MONTH);
                            month = month + 1;
                            mes = Integer.toString(month);
                            anno = Integer.toString(calendario.get(Calendar.YEAR));

                            fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
                            comment.setFecha_creacion(fechaCon);
                            
                            comment.setmensaje(contenido);
                            comment.setnickName(nickName);

                            dao.insertComentario(comment);
                            dao.insertarComentarioSuperColumn(comment);


                            System.out.println("Va a saveAttachedFile");
                            System.out.println("long: "+request.getContentLength());
                            saveAttachedFile(request);

                            return comment;          
                            
                        }
                        else {
                            
                            System.out.println("Token vencido");
                            comment.setId("ERROR 100: Token vencido");
                            comment.setFecha_creacion("ERROR 100");
                            comment.setAdjunto("ERROR 100");
                            comment.setmensaje("ERROR 100");
                            comment.setnickName("ERROR 100");
                            
                            return comment;
                            
                        }
                        
                    }
                    else{
                            System.out.println("Token vencido");
                            comment.setId("ERROR 100: Token vigente");
                            comment.setFecha_creacion("ERROR 100");
                            comment.setAdjunto("ERROR 100");
                            comment.setmensaje("ERROR 100");
                            comment.setnickName("ERROR 100");
                            
                            return comment;
                    }
                   } else {
                            System.out.println("El token no corresponde con el usuario");
                            comment.setId("ERROR 150: El token no corresponde con el usuario");
                            comment.setFecha_creacion("ERROR 150");
                            comment.setAdjunto("ERROR 150");
                            comment.setmensaje("ERROR 150");
                            comment.setnickName("ERROR 150");
                            
                            return comment;
                       
                   }
                   
               }
               
                            System.out.println("Token incorrecto");
                            comment.setId("ERROR 111: Token incorrecto");
                            comment.setFecha_creacion("ERROR 111");
                            comment.setAdjunto("ERROR 111");
                            comment.setmensaje("ERROR 111");
                            comment.setnickName("ERROR 111");
                       
                            return comment;
               }
               else{
                   
                            System.out.println("El usuario no existe");
                            comment.setId("ERROR 211");
                            comment.setFecha_creacion("ERROR 211");
                            comment.setAdjunto("ERROR 211");
                            comment.setmensaje("ERROR 211");
                            comment.setnickName("ERROR 211: El usuario no existe");
                            
                            return comment;
                   
               }       
                
//////////                  if (!isMultipart) {
//////////            //out.print(stream.toXML(new ErrorMessage("003",
//////////                //    "El request recibido no contiene ningun archivo")));
//////////
//////////           // logUtility.warn("IdUsuario=" + tokenUsuario.getIdUsuario()
//////////                //    + " solicitando token desde=" + tokenUsuario.getHostSolicitante());
//////////            //  System.out.println("no soy multi part mi pana");
//////////                comment.setmensaje("ERROR 015: El request recibido no contiene ningun archivo");
//////////                comment.setnickName("ERROR 015");
//////////                      System.out.println("Entro en el if de multipart");
//////////                    return comment;
//////////                }
                
                     
                     
                     
                     
////////////////                dao = new comentarioDAOImpl();
////////////////                
////////////////                String idD = dao.getID();
////////////////                System.out.println("ID mas grande de la base de datos: "+idD);
////////////////                
////////////////                int codigoID;
////////////////                codigoID = Integer.parseInt(idD);
////////////////                codigoID = codigoID + 1;
////////////////                
////////////////                idD = String.valueOf(codigoID);
////////////////                System.out.println("ID a enviar al dao: "+idD);
////////////////                comment.setId(idD);
////////////////                comment.setReply("0");
////////////////                comment.setAdjunto("Si");
////////////////                
////////////////                Calendar calendario = Calendar.getInstance();
////////////////                String horas, minutos;
////////////////                int month;
////////////////                String dia, mes, anno, fechaCon;
////////////////                
////////////////                horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
////////////////                minutos = Integer.toString(calendario.get(Calendar.MINUTE));
////////////////                dia = Integer.toString(calendario.get(Calendar.DATE));
////////////////                month = calendario.get(Calendar.MONTH);
////////////////                month = month + 1;
////////////////                mes = Integer.toString(month);
////////////////                anno = Integer.toString(calendario.get(Calendar.YEAR));
////////////////                
////////////////                fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
////////////////                comment.setFecha_creacion(fechaCon);
////////////////                
////////////////                dao.insertComentario(comment);
////////////////                dao.insertarComentarioSuperColumn(comment);
////////////////                
////////////////               
////////////////                System.out.println("Va a saveAttachedFile");
////////////////                System.out.println("long: "+request.getContentLength());
////////////////                saveAttachedFile(request);
////////////////		
////////////////		return comment;

	}
    
    
    
      private void saveAttachedFile(HttpServletRequest request) {

        File filePath = new File(DIR_REPOSITORIO);
        
        int maxFileSize = 100000 * 1024;
        
        int maxMemSize = 4000 * 1024;
        
        File file;
           //  System.out.println("uhmmmmmmm");

        DiskFileItemFactory factory = new DiskFileItemFactory();
        
        // maximum size that will be stored in memory
        factory.setSizeThreshold(maxMemSize);
        
        // Location to save data that is larger than maxMemSize.
        factory.setRepository(filePath);
        
        // Create a new file upload handler
     //   ServletFileUpload upload = new ServletFileUpload(factory);
               ServletFileUpload upload = new ServletFileUpload(factory);
 
        // maximum file size to be uploaded.
        upload.setSizeMax(maxFileSize);
      // System.out.println("antes del try"+ request.toString());
        // System.out.println("prueba"+ upload.parseRequest(request));
      // System.out.println(upload.toString());
        try {
            // Parse the request to get file items.
            List fileItems = upload.parseRequest(request);
              
            System.out.println("entre a try");

            // Process the uploaded file items
            Iterator i = fileItems.iterator();

            while (i.hasNext()) {
                FileItem fi = (FileItem) i.next();
                      //System.out.println("hay peooooo?????"+ fi);

                if (!fi.isFormField()) {
                    // Get the uploaded file parameters
                    String fieldName = fi.getFieldName();

                    String fileName = fi.getName();

                    String contentType = fi.getContentType();

                    boolean isInMemory = fi.isInMemory();

                    long sizeInBytes = fi.getSize();

                    // Write the file
                    if (fileName.lastIndexOf("\\") >= 0) {
                        file = new File(filePath,
                                fileName.substring(fileName.lastIndexOf("\\")));
                    } else {
                        file = new File(filePath,
                                fileName.substring(fileName.lastIndexOf("\\") + 1));
                    }

                    fi.write(file);

                   // logUtility.info("Uploaded Filename: " + fileName);
                }
            }

        } catch (Exception ex) {
           // logUtility.error("Error al procesar archivo adjunto", ex);
        }
    }  
    
    
    
    
}
