package ni.com.sts.estudioCohorteCSSFV.ftp;

import ni.com.sts.estudioCohorteCSSFV.util.InfoResultado;
import ni.com.sts.estudioCohorteCSSFV.util.UtilLog;
import ni.com.sts.estudioCohorteCSSFV.util.UtilProperty;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

	
public class FTPConnection {
	private final Logger logger = Logger.getLogger(this.getClass());
	private CompositeConfiguration config;
	public InfoResultado downloadFile(FTPParams params){
		config = UtilProperty.getConfigurationfromExternalFile("estudioCohorteCSSFVAD.properties");
		UtilLog.setLog(config.getString("estudioCohorteCSSFVAD.log"));
		InfoResultado resultado = new InfoResultado();
		FTPClient ftpClient = new FTPClient();
        try {
        	logger.info("FTP Connection Params");
        	logger.info("Server: "+params.getServer());
        	logger.info("Port: "+params.getPort());
        	logger.info("User: "+params.getUser());
        	logger.info("Pass: "+params.getPass());
        	logger.info("File: "+params.getFileName());
        	logger.info("LocalPath: "+params.getLocalFilePath());
        	logger.info("RemotePath: "+params.getRemoteFilePath());
        	
        	
            ftpClient.connect(params.getServer(), params.getPort());
            ftpClient.login(params.getUser(), params.getPass());
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
 
            // APPROACH #1: using retrieveFile(String, OutputStream)
            String remoteFile = params.getRemoteFilePath()+params.getFileName();
            String localFile = params.getLocalFilePath()+params.getFileName();
            /*logger.info("descarga inicia");
            File downloadFile1 = new File(localFile);
            OutputStream outputStream1 = new BufferedOutputStream(new FileOutputStream(downloadFile1));
            boolean success = ftpClient.retrieveFile(remoteFile, outputStream1);
            outputStream1.close();
 			logger.info("descarga terminada");
            if (success) {
                System.out.println("File #1 has been downloaded successfully.");                
            }
            resultado.setOk(success);
            resultado.setMensaje("File #1 has been downloaded successfully.");*/
 
            // APPROACH #2: using InputStream retrieveFileStream(String)
            //String remoteFile2 = "/test/song.mp3";
            logger.info("descarga inicia");
            File downloadFile2 = new File(localFile);
            OutputStream outputStream2 = new BufferedOutputStream(new FileOutputStream(downloadFile2));
            InputStream inputStream = ftpClient.retrieveFileStream(remoteFile);
            byte[] bytesArray = new byte[4096];
            int bytesRead = -1;
            while ((bytesRead = inputStream.read(bytesArray)) != -1) {
                outputStream2.write(bytesArray, 0, bytesRead);
            }
 
            boolean success = ftpClient.completePendingCommand();
            logger.info("descarga terminada");
            if (success) {
                System.out.println("File "+params.getFileName()+" has been downloaded successfully.");
            }
            resultado.setOk(success);
            resultado.setMensaje("File "+params.getFileName()+" has been downloaded successfully.");
            outputStream2.close();
            inputStream.close();
 
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            logger.error(ex);
            ex.printStackTrace();
            resultado.setOk(false);
            resultado.setMensaje(ex.getMessage());
        }catch(Throwable thr){
        	System.out.println("Error: " + thr.getMessage());
        	logger.error(thr);
        	thr.printStackTrace();
            resultado.setOk(false);
            resultado.setMensaje(thr.getMessage());
        }finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
            	logger.error(ex);
                ex.printStackTrace();
                resultado.setOk(false);
                resultado.setMensaje(ex.getMessage());
            }            
        }
        return resultado;
	}

	public InfoResultado downloadFileSFTP(FTPParams params){
		config = UtilProperty.getConfigurationfromExternalFile("estudioCohorteCSSFVAD.properties");
		UtilLog.setLog(config.getString("estudioCohorteCSSFVAD.log"));
		InfoResultado resultado = new InfoResultado();
		Session     session     = null;
		Channel     channel     = null;
		ChannelSftp channelSftp = null;
		OutputStream os = null;
		BufferedInputStream bis = null;
		BufferedOutputStream bos = null;
        try {
        	logger.info("SFTP Connection Params");
        	logger.info("Server: "+params.getServer());
        	logger.info("Port: "+params.getPort());
        	logger.info("User: "+params.getUser());
        	logger.info("Pass: "+params.getPass());
        	logger.info("File: "+params.getFileName());
        	logger.info("LocalPath: "+params.getLocalFilePath());
        	logger.info("RemotePath: "+params.getRemoteFilePath());
        	
        	JSch jsch = new JSch();
        	session = jsch.getSession(params.getUser(),params.getServer(),params.getPort());
        	session.setPassword(params.getPass());
        	java.util.Properties config = new java.util.Properties();
        	config.put("StrictHostKeyChecking", "no");
        	session.setConfig(config);
        	session.connect();
        	channel = session.openChannel("sftp");
        	channel.connect();
        	channelSftp = (ChannelSftp)channel;
        	channelSftp.cd(params.getRemoteFilePath());
        	     	
            
 
            // APPROACH #1: using retrieveFile(String, OutputStream)
            String remoteFile = params.getRemoteFilePath()+params.getFileName();
            String localFile = params.getLocalFilePath()+params.getFileName();
            
            byte[] buffer = new byte[1024];
        	bis = new BufferedInputStream(channelSftp.get(params.getFileName()));
        	File newFile = new File(localFile);
        	os = new FileOutputStream(newFile);
        	bos = new BufferedOutputStream(os);
        	int readCount;
        	//System.out.println("Getting: " + theLine);
        	while( (readCount = bis.read(buffer)) > 0) {        	
        		bos.write(buffer, 0, readCount);
        	}
        	bos.flush();
        	resultado.setOk(true);
        	resultado.setMensaje("archivo descargado");
 
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            logger.error(ex);
            ex.printStackTrace();
            resultado.setOk(false);
            resultado.setMensaje(ex.getMessage());
        }catch(Throwable thr){
        	System.out.println("Error: " + thr.getMessage());
        	logger.error(thr);
        	thr.printStackTrace();
            resultado.setOk(false);
            resultado.setMensaje(thr.getMessage());
        }finally {
            try {
            	if (channelSftp != null)
            	   {
            	       channelSftp.disconnect();
            	   }
            	if( os!=null ) 
                {
                    os.close();
                }
                if( bis!=null ) 
                {
                    bis.close();
                }
                if( bos!=null ) 
                {
                    bos.close();
                }
            } catch (IOException ex) {
            	logger.error(ex);
                ex.printStackTrace();
                resultado.setOk(false);
                resultado.setMensaje(ex.getMessage());
            }            
        }
        return resultado;
	}


}
