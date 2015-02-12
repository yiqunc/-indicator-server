package au.edu.csdila.indicator;


import spark.*;

import java.io.File;
import java.io.IOException;
import org.geotools.feature.SchemaException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;


import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.csdila.common.GeoServerService;
import au.edu.csdila.indicator.iia.iia;
import au.edu.csdila.indicator.iop.*;
import au.edu.csdila.indicator.isa.*;
public class Controller {
		
	static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);
	
		/**
		 * Prepare params for IOP, then run IOP and finally publish IOP results through GeoServer service
		 * @param request
		 * @param response
		 * @return
		 * @throws IOException
		 * @throws JSONException
		 * @throws InterruptedException
		 * @throws NoSuchAuthorityCodeException
		 * @throws FactoryException
		 * @throws SchemaException
		 */
	    static public JSONObject runIOP(Request request,
	    		Response response) throws Exception {
			synchronized (request.session()) {
				
				//final outputs (to UI)
				JSONObject output = new JSONObject();
				
				// input model parameters json (from UI)
				JSONObject options = null;
				
				// model output json
				JSONObject modelOutput = null;
				
				// geoserver publish output json
				JSONArray geoServerServiceOutput = null;
								
				//get paras from request
				options =  new JSONObject(request.queryParams("simpara"));
				options.put("simid",Integer.parseInt(request.queryParams("simid")));
				options.put("simcode",request.queryParams("simcode"));
				
				//add simid,username,tmpOutputFolderPath
				options.put("userid", request.session().attribute("userid"));
				options.put("username", request.session().attribute("username"));
				options.put("tmpOutputFolderPath", getOutputFolderPath("tmpout/iop",request));
				
				//run the algorithm 
				modelOutput = iop.exec(options);
				
				//feed modelouputs to geoserver publish service
				geoServerServiceOutput = GeoServerService.publishShpArr(modelOutput.getJSONArray("vectors"), options);
				
				//prepare the output json
				output.put("infos", modelOutput.getJSONArray("infos"));
				output.put("vectors", geoServerServiceOutput);
				output.put("status", 0);
				output.put("errdesc", "");
				
				//save result in db
				DbAccess.updateSimResult(options.getInt("simid"), output, "simoutputs");
				
				//update sim state
				DbAccess.updateSimState(options.getInt("simid"), 2);
				
				return output;
			}
		}
	    
		/**
		 * Prepare params for ISA, then run ISA and finally publish ISA results through GeoServer service
		 * @param request
		 * @param response
		 * @return
		 * @throws Exception 
		 */
	    static public JSONObject runISA(Request request,
	    		Response response) throws Exception {
			synchronized (request.session()) {
				
				//final outputs (to UI)
				JSONObject output = new JSONObject();
				
				// input model parameters json (from UI)
				JSONObject options = null;
				
				// model output json
				JSONObject modelOutput = null;
				
				// geoserver publish output json
				JSONArray geoServerServiceOutput = null;
				
				//get paras from request
				options =  new JSONObject(request.queryParams("simpara"));
				options.put("simid",Integer.parseInt(request.queryParams("simid")));
				options.put("simcode",request.queryParams("simcode"));
				
				//add simid,username,tmpOutputFolderPath
				options.put("userid", request.session().attribute("userid"));
				options.put("username", request.session().attribute("username"));
				options.put("tmpOutputFolderPath", getOutputFolderPath("tmpout/isa",request));
				
				//run the algorithm 
				modelOutput = isa.exec(options);
				
				//feed modelouputs to geoserver publish service
				geoServerServiceOutput = GeoServerService.publishShpArr(modelOutput.getJSONArray("vectors"), options);
				
				//prepare the output json
				output.put("infos", modelOutput.getJSONArray("infos"));
				output.put("vectors", geoServerServiceOutput);
				output.put("status", 0);
				output.put("errdesc", "");
				
				//save result in db
				DbAccess.updateSimResult(options.getInt("simid"), output, "simoutputs");
				
				//update sim state
				DbAccess.updateSimState(options.getInt("simid"), 2);
				
				return output;
			}
		}
	    
	    
		/**
		 * Prepare params for IIA, then run IIA and finally publish IIA results through GeoServer service
		 * @param request
		 * @param response
		 * @return
		 * @throws Exception 
		 */
	    static public JSONObject runIIA(Request request,
	    		Response response) throws Exception {
			synchronized (request.session()) {
				
				//final outputs (to UI)
				JSONObject output = new JSONObject();
				
				// input model parameters json (from UI)
				JSONObject options = null;
				
				// model output json
				JSONObject modelOutput = null;
				
				// geoserver publish output json
				JSONArray geoServerServiceOutput = null;
								
				//get paras from request
				options =  new JSONObject(request.queryParams("simpara"));
				options.put("simid",Integer.parseInt(request.queryParams("simid")));
				options.put("simcode",request.queryParams("simcode"));
				
				//add simid,username,tmpOutputFolderPath
				options.put("userid", request.session().attribute("userid"));
				options.put("username", request.session().attribute("username"));
				options.put("tmpOutputFolderPath", getOutputFolderPath("tmpout/iia",request));
				
				//run the algorithm 
				modelOutput = iia.exec(options);
				
				//feed modelouputs to geoserver publish service
				geoServerServiceOutput = GeoServerService.publishShpArr(modelOutput.getJSONArray("vectors"), options);
				
				//prepare the output json
				output.put("infos", modelOutput.getJSONArray("infos"));
				output.put("vectors", geoServerServiceOutput);
				output.put("status", 0);
				output.put("errdesc", "");
				
				//save result in db
				DbAccess.updateSimResult(options.getInt("simid"), output, "simoutputs");
				
				//update sim state
				DbAccess.updateSimState(options.getInt("simid"), 2);
				
				return output;
			}
		}
	    
	    /**
	     * Get output folder path string for current login user, if target output folder doesn't exist, create a new one
	     * @param dirname
	     * @param request
	     * @return output folder path string for current login user
	     */
	    public static String getOutputFolderPath(String dirname, Request request){
	    	
	    	File f = new File(request.raw().getServletContext().getRealPath("/")+"/"+dirname+"/"+request.session().attribute("username"));
        	if(!f.exists()){
        		
        		boolean flag = f.mkdirs();
        		
        		System.out.println("=== make new folder "+ flag);

        	}
        	
        	return f.getAbsolutePath();
	    }
	    
}
