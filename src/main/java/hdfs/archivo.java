package hdfs;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.Scanner; //scanner


public class archivo {
	
	static Scanner scan = new Scanner(System.in);

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		int option=0;
		
	
        
        do{   	
            System.out.println("====================================================");
            System.out.println("              Elige una opción");
            System.out.println("  Elige 1 si deseas crear un nuevo archivo");
            System.out.println("  Elige 2 si deseas eliminar el archivo");
            System.out.println("  Elige 3 si deseas salir");
      
            System.out.println("====================================================");
            
            
            System.out.println("Escribe un número");
            option = Integer.parseInt(scan.nextLine()); //leer opcion de usuario
            
            archivo.opciones(option);
            
        }while(option != 3 );
	}	
	
	//switch
	static void opciones(int op) throws IOException{
	     
	      switch(op){
	           
	            case 1 :
	              // Declaraciones
	              archivo.crearArchivo();
	              break; // break es opcional
	           
	            case 2 :
	              // Declaraciones
	            	archivo.eliminarArchivo();
	              break; // break es opcional
	              
	            case 3 :
	              // Declaraciones
	              System.out.println("Saliendo.....");
	              System.out.println(".............");
	              System.out.println("Se cerro el programa con exito!!");
	              break; // break es opcional
	              
	           
	           
	           default : 
	              
	        }
	      
	    }//fin switch
	
	public static void crearArchivo() {
		
		//Creamos la configuración de acceso al HDFS
		Configuration conf = new Configuration(true);
		conf.set("fs.defaultFS", "hdfs://192.168.1.65:8020/");
					
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		
		String nuevaCarpeta = "";
		String nombreArchivo = "";
		String contenido = "";
		
		//String rutaHDFS = "/Ubyquit";
		String rutaHDFS = "";
		
		//String rutaLocal =  "/home/cloudera/Ubyquit";
		String rutaLocal =  "";
		//String archivoLocal = "/home/cloudera/Ubyquit/Local";
		String archivoLocal = "";
		
		String propietario ="";
		String pwdPropietario="";
		
		String permiso="";
		
		System.out.println("Escribe el nombre de la carpeta");
		nuevaCarpeta = scan.nextLine(); 
		System.out.println("");
		System.out.println("==================================");
		System.out.println("");
		System.out.println("Escribe el nombre del archivo");
		nombreArchivo = scan.nextLine(); 
		System.out.println("");
		System.out.println("==================================");
		System.out.println("");
		System.out.println("Escribe el contenido del archivo");
		contenido = scan.nextLine(); 
		System.out.println("");
		System.out.println("==================================");
		System.out.println("");
		System.out.println("Escribe la ruta HDFS");
		rutaHDFS = scan.nextLine(); 
		System.out.println("");
		System.out.println("==================================");
		System.out.println("");
		System.out.println("Escribe la ruta local");
		rutaLocal = scan.nextLine(); 
		System.out.println("");
		System.out.println("==================================");
		System.out.println("");
		System.out.println("Escribe la ruta del archivo local");
		archivoLocal = scan.nextLine(); 
	
		
		
		try {
			//Crear objeto FileSystem
			FileSystem fs = FileSystem.get(conf);
			
			String home = fs.getHomeDirectory().toString();
			
			//En caso de que no exista la carpeta, crear la carpeta.
			if(!fs.exists(new Path(home + "/" + nuevaCarpeta))) {
				fs.mkdirs(new Path(home + "/" + nuevaCarpeta));
			}
			
			//Si no existe el archivo, hay que crearlo*/
			Path rutaArchivo = new Path(home + "/" + nuevaCarpeta + "/" + nombreArchivo);
			FSDataOutputStream outputStream = null;
			
			if(!fs.exists(rutaArchivo)) {
				outputStream = fs.create(rutaArchivo);
				outputStream.writeBytes(contenido);
				outputStream.close();
			}
			
			//Vamos a leer el archivo que acabamos de escribir.
			Path rutaarchivo = new Path(rutaHDFS + "/" + nombreArchivo);

			FSDataInputStream inputStream = fs.open(rutaarchivo);
			String salida = IOUtils.toString(inputStream, "UTF-8");
			inputStream.close();

			System.out.println(salida);
			
			//Podemos ver el estado del archivo.
			FileStatus status = fs.getFileStatus(rutaArchivo);
		
			
			//Tambien podemos modificar el propietario o los permisos del archivo.
			System.out.println("");
			System.out.println("==================================");
			System.out.println("");
			System.out.println("Escribe el propietario");
			System.out.println("");
			propietario = scan.nextLine();
			System.out.println("==================================");
			System.out.println("");
			System.out.println("Escribe el el pwsd de propietario");
			System.out.println("");
			pwdPropietario = scan.nextLine();

			fs.setOwner(rutaArchivo, propietario, pwdPropietario);
			
				
			System.out.println("");
			System.out.println("==================================");
			System.out.println("");
			System.out.println("Escribe los permisos");
			System.out.println("");
			permiso = scan.nextLine();
			
			
			FsPermission permisos = FsPermission.valueOf(permiso);
			//FsPermission permisos = FsPermission.valueOf("-rwxrwxrwx");
			fs.setPermission(rutaArchivo, permisos);
			
			//Al igual que hemos realizado con la línea de comandos, podemos mover archivo de Local al HDFS y viceversa.
			//Local --> HDFS
			
			fs.copyFromLocalFile(false, true, new Path(archivoLocal), new Path(rutaHDFS));
			//HDFS --> Local
			fs.copyToLocalFile(false, rutaArchivo, new Path(rutaLocal));
			} 
			catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
		
	}
	
	
	public static void eliminarArchivo() throws IOException {
		
		String rutaHDFS="";
		
		//Creamos la configuración de acceso al HDFS
		Configuration conf = new Configuration(true);
		conf.set("fs.defaultFS", "hdfs://192.168.1.65:8020/");
		
		//Crear objeto FileSystem
		FileSystem fs = FileSystem.get(conf);
		//Por ultimo, borraremos el directorio y los archivos.
		System.out.println("==================================");
		System.out.println("Escribe la rutaHDFS");
		System.out.println("");
		rutaHDFS = scan.nextLine();
		
		fs.delete(new Path(rutaHDFS), true);
		fs.close(); 
		
		System.out.println(rutaHDFS);
		System.out.println("borrando archivo...");
		System.out.println("...");
		System.out.println("archivo borrado!!");
	}
	
		
}			

