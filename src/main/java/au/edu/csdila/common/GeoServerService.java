package au.edu.csdila.common;

import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher.UploadMethod;
import it.geosolutions.geoserver.rest.GeoServerRESTReader;
import it.geosolutions.geoserver.rest.encoder.GSLayerEncoder;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder.ProjectionPolicy;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.csdila.common.Zip;


public class GeoServerService {
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerService.class);

	private final static String wfsUrlTemplate = "%s/wfs?service=wfs&version=1.0.0&request=GetFeature&typeName=%s:%s&outputFormat=json"; 
	public static GeoServerRESTPublisher publisher;
	public static GeoServerRESTReader reader;
	
	public static void init(){
		
		if(isGeoServerReachable()){
			
			LOGGER.info("GEOSERVER_CONNECTED_SUCCESSFULLY");
			
		}
		
	}
	
	
	public static boolean isGeoServerReachable(){

		try {
			
			reader = new GeoServerRESTReader(AppConfig.getString("geoserverRESTURL"), AppConfig.getString("geoserverRESTUSER"), AppConfig.getString("geoserverRESTPW"));
			publisher = new GeoServerRESTPublisher(AppConfig.getString("geoserverRESTURL"), AppConfig.getString("geoserverRESTUSER"), AppConfig.getString("geoserverRESTPW"));
		
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			LOGGER.error("ERROR_CONNECT_GEOSERVER");
			return false;
		}

		if (reader == null || !reader.existGeoserver()) {
			LOGGER.error("GEOSERVER_NOT_RUNNING");
			return false;
		}
		
		return true;
	}
	
	public static String publishShp(String workspace, String dataStore,
			String layer, String srs, String nativeCRS, File newFile) throws FileNotFoundException, IllegalArgumentException{
		
		//step1 check if target workspace exists, if not,create a new workspace
		List<String> existingWorkSpaces = reader.getWorkspaceNames();
		if (!existingWorkSpaces.contains(workspace)) {
			publisher.createWorkspace(workspace);
			LOGGER.info("WORKSPACE_NOT_EXIST_BUT_CREATED");
		}
		
		//step2 check if target layer esists, if yes, unpublish it and then remove the existing one first
		if (reader.getLayer(workspace, layer) != null) {
			publisher.unpublishFeatureType(workspace, dataStore, layer);
			publisher.removeLayer(workspace, layer);
		}
		
		//step3 create a zip file for publish
		String zipfileName = newFile.getAbsolutePath().substring(0,newFile.getAbsolutePath().lastIndexOf("."))+ ".zip";
		LOGGER.info("zipfileName :{}", zipfileName);
		Zip zip = new Zip(zipfileName, newFile.getParentFile().getAbsolutePath());
		zip.createZip();
		LOGGER.info("zipdone!");
		File zipFile = new File(zipfileName);
		
		//step4 publish shp file with srs projection  
		boolean published = true;
		published = publisher.publishShp(workspace, dataStore, null, layer, UploadMethod.FILE, zipFile.toURI(), srs, nativeCRS, ProjectionPolicy.REPROJECT_TO_DECLARED, null);
		
		if(published){

			LOGGER.info("published");
		} else
		{
			LOGGER.info("error in published");
		}
		
		return String.format(wfsUrlTemplate, AppConfig.getString("geoserverRESTURL"), workspace, layer);
	}
	
	public static JSONArray publishShpArr(JSONArray vectors, JSONObject options) throws JSONException, FileNotFoundException, IllegalArgumentException{
		
		
			 for(int j = 0; j < vectors.length(); j++){
				 
				 String forcesrs = vectors.getJSONObject(j).getString("forcesrs");
				 if(forcesrs==null || forcesrs.length()==0) forcesrs = "EPSG:4326";
				 String nativesrs = vectors.getJSONObject(j).getString("nativesrs");
				 if(nativesrs==null || nativesrs.length()==0) nativesrs = "EPSG:4326";
				 
				 String url = publishShp(options.getString("username"), 
						 options.getString("simcode"),
						 vectors.getJSONObject(j).getString("name"),
						 forcesrs, nativesrs, 
						 new File(vectors.getJSONObject(j).getString("path"))
						 );
				 vectors.getJSONObject(j).put("gsurl", url);
			 }
		
		
		return vectors;
	}
	
	// the publish service output structure:
	/*
	 * {
	 * 		status:0,
	 * 		errdesc:"error description",
	 * 		data:{
	 * 				vectors:[{
	 * 							name:
	 * 							path:
	 * 							gsurl:
	 * 						}],
	 * 				rasters:[],
	 * 				statistics:[]
	 * 			}
	 * }
	 * 
	 * 
	 * 
	 * */
	public static JSONObject publish(JSONObject data, JSONObject options) throws JSONException{
		
		JSONObject rlt = new JSONObject();
		
		JSONArray vectors;
		try {
			vectors = data.getJSONArray("vectors");
			for(int j = 0; j < vectors.length(); j++){
				 
				 String url = publishShp(options.getString("username"), 
						 options.getString("simcode"),
						 vectors.getJSONObject(j).getString("name"),
						 "EPSG:4326", "EPSG:4326", 
						 new File(vectors.getJSONObject(j).getString("path"))
						 );
				 data.getJSONArray("vectors").getJSONObject(j).put("gsurl", url);
			 }
			
			rlt.put("status", 0);
			rlt.put("errdesc", "");
			rlt.put("data", data);
			return rlt;
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		rlt.put("status", 1);
		rlt.put("errdesc", "ERR OCCURS IN: GeoServerService");
		
		return rlt;
	}
	
}
