package edu.hnu.cg.graph.config;
import java.util.MissingResourceException;
import java.util.ResourceBundle;


public class Configure {
	
	private static  String configPath = "config";
	private ResourceBundle resourceBundle;
	
	public static Configure getConfigure(){
		return ConfigureInstanceHolder.configure;
	}
	
	private static class ConfigureInstanceHolder{
		private static Configure configure = new Configure();
	}
	
	private Configure(){
		resourceBundle = ResourceBundle.getBundle(configPath);
	}
	
	private String getString(String key){
		if(key == null || key.equals("") || key.equals("null")){
			return null;
		}
		String result = null;
		
		try{
			result = resourceBundle.getString(key);
		}catch(MissingResourceException e){
			e.printStackTrace();
		}
		
		return result;
	}
	
	public String getStringValue(String key){
		return getString(key);
	}
	
	public int getInt(String key){
		String r = getString(key);
		int value = Integer.parseInt(r);
		return value;
	}
	
	public static void main(String[] args) {
		Configure config = Configure.getConfigure();
		String path = config.getStringValue("graphfile");
		System.out.println(path);
	}

}
