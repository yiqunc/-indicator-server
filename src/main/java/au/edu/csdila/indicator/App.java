package au.edu.csdila.indicator;

import static spark.Spark.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import net.opengis.ows10.MetadataType;
import net.opengis.wps10.ProcessBriefType;
import net.opengis.wps10.ProcessDescriptionType;
import net.opengis.wps10.ProcessDescriptionsType;
import net.opengis.wps10.ProcessOfferingsType;
import net.opengis.wps10.WPSCapabilitiesType;

import org.apache.commons.io.FileUtils;
import org.eclipse.emf.common.util.EList;
import org.geotools.data.DataSourceException;
import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.data.wps.WPSFactory;
import org.geotools.data.wps.WebProcessingService;
import org.geotools.data.wps.request.DescribeProcessRequest;
import org.geotools.data.wps.response.DescribeProcessResponse;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.graph.path.Path;
import org.geotools.ows.ServiceException;
import org.geotools.parameter.Parameters;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.Filter;
import org.opengis.filter.spatial.Intersects;
import org.opengis.geometry.MismatchedDimensionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;

import spark.servlet.*;
import spark.*;


import au.edu.csdila.common.*;


import au.edu.csdila.common.WFSDataStore;


import org.geotools.process.feature.*;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;

import org.geotools.data.crs.ForceCoordinateSystemFeatureResults;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.*;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.*;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import  com.vividsolutions.jts.geom.Geometry;

import au.edu.csdila.indicator.iop.*;

import org.geotools.data.store.ReprojectingFeatureCollection;
public class App implements SparkApplication {

	static final Logger LOGGER = LoggerFactory.getLogger(App.class);
	
	//config for running Spark on external Web Server, e.g. Jetty (which is not the Spark embedded Jetty version)
	public void init() {
		
		//must have this line, otherwise, the OasAnalysisRiskAra result published in geoserver will get lon-lat reversed, i.e. lat-lon
		//ref: http://docs.geotools.org/latest/userguide/faq.html search 'org.geotools.referencing.forceXY'
		System.setProperty("org.geotools.referencing.forceXY", "true");
		//load application configurations
		AppConfig.loadConfig();
		GeoServerService.init();
		DbAccess.initPostgresDataSource();

		/**
		 * Service for login session test. 
		 */
		get(new Route("/islogined") {
	     @Override
	     public Object handle(Request request, Response response) {
	     	
	     	
	     	if(request.session() !=null && request.session().attribute("username") != null){
	     		
	     		return "{status:'ok', username:'"+request.session().attribute("username")+"', jsessionid:'"+request.session().raw().getId()+"'}";
	     	}
	     	return "{status:'error', desc:'not logged in yet.'}";
	     }});
		
		/**
		 * Service for user login.
		 */
		post(new Route("/dologin") {
	     @Override
	     public Object handle(Request request, Response response) {
	    	 try{
		        	// check username/pswd
		        	JSONObject result = DbAccess.userLogin(request.queryParams("username"), request.queryParams("userpswd"));
	
		        	if (result.getInt("userid") > 0){
		        		
			        	request.session().attribute("userid", result.getInt("userid"));
			        	request.session().attribute("username", result.getString("username"));
		        		return "{status:'ok', username:'"+request.session().attribute("username")+"', jsessionid:'"+request.session().raw().getId()+"'}";
	
		        	}
		        	else{
		        		
		        		return "{status:'error', desc:'wrong username and password.'}";
		        		
		        	}
		        	
	        	} catch(JSONException e){
	        		
	        		return "{status:'error', desc:'json parsing exception.'}";
	        	}
	     	
	     }});
		
		/**
		 * Service for user log out. 
		 */
		get(new Route("/dologout") {
	     @Override
	     public Object handle(Request request, Response response) {
	
	     			LOGGER.info("user {} is logged out",request.session().attribute("username"));
	     			request.session().invalidate();
		        		return "{status:'ok'}";
	
	     }});
			
		/**
		 * Service for listing results for each type of analysis for current login user
		 * @param maxnum: max number of results to be fetched. 
		 * @param modelid: analysis(model) type id: 1-IOP, 2-ISA, 3-IIA
		 * @param userid: login user id, obtained from current session
		 * @return a JSON array containing the results.   
		 */
		get(new Route("/services/results") {
	        @Override
	        public Object handle(Request request, Response response) {
	        	
	        	int maxnum = Integer.parseInt(request.queryParams("maxnum"));
	        	int modelid = Integer.parseInt(request.queryParams("modelid"));
				int userid = Integer.parseInt(request.session().attribute("userid").toString());
				return DbAccess.getSimRecords(maxnum, modelid, userid).toString();
	        	
	        }});
		
		/**
		 * Service for initializing a IOP job in the database
		 * @param options: client defined params for running IOP. 
		 * @param userid: login user id, obtained from current session
		 * @return a JSON object containing newly create job(sim) id.   
		 */
		post(new Route("/services/iop/init") {
	        @Override
	        public Object handle(Request request, Response response) {
	        	
	        	String params = request.queryParams("params");
	        	JSONObject options = null;
				try {
					options = new JSONObject(String.format("{userid:%d, modelid:1, options:%s}", request.session().attribute("userid"), params));
					LOGGER.info("===options: {}", options.toString());
					return String.format("{simid:%d}", DbAccess.createSim(options));
					
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	return "{simid:-1}";
	        	
	        	
	        }});
		
		/**
		 * Service for executing a IOP job
		 * @return a JSON object containing algorithm outputs.   
		 */
		post(new Route("/services/iop/exec") {
	        @Override
	        public Object handle(Request request, Response response) {
	    
        		JSONObject data = new JSONObject();
        		try {
        			data = Controller.runIOP(request, response);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	        	
	        	JSONObject output = new JSONObject();
	        	try {
					output.put("status", true);
					output.put("data", data);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	           return output.toString();
	        }
	     });
		
		/**
		 * Service for initializing a ISA job in the database
		 * @param options: client defined params for running ISA. 
		 * @param userid: login user id, obtained from current session
		 * @return a JSON object containing newly create job(sim) id.   
		 */
		post(new Route("/services/isa/init") {
	        @Override
	        public Object handle(Request request, Response response) {
	        	
	        	String params = request.queryParams("params");
	        	JSONObject options = null;
				try {
					options = new JSONObject(String.format("{userid:%d, modelid:2, options:%s}", request.session().attribute("userid"), params));
					LOGGER.info("===options: {}", options.toString());
					return String.format("{simid:%d}", DbAccess.createSim(options));
					
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	return "{simid:-1}";
	        	
	        	
	        }});
		
		/**
		 * Service for executing a ISA job
		 * @return a JSON object containing algorithm outputs.   
		 */
		post(new Route("/services/isa/exec") {
	        @Override
	        public Object handle(Request request, Response response) {
	        
        		JSONObject data = new JSONObject();
        		try {
        			data = Controller.runISA(request, response);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	JSONObject output = new JSONObject();
	        	try {
					output.put("status", true);
					output.put("data", data);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	           return output.toString();
	        }
	     });
		
		/**
		 * Service for initializing a IIA job in the database
		 * @param options: client defined params for running IIA. 
		 * @param userid: login user id, obtained from current session
		 * @return a JSON object containing newly create job(sim) id.   
		 */
		post(new Route("/services/iia/init") {
	        @Override
	        public Object handle(Request request, Response response) {
	        	
	        	String params = request.queryParams("params");
	        	JSONObject options = null;
				try {
					options = new JSONObject(String.format("{userid:%d, modelid:3, options:%s}", request.session().attribute("userid"), params));
					LOGGER.info("===options: {}", options.toString());
					return String.format("{simid:%d}", DbAccess.createSim(options));
					
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	return "{simid:-1}";
	        	
	        	
	        }});
		
		/**
		 * Service for executing a IIA job
		 * @return a JSON object containing algorithm outputs.   
		 */
		post(new Route("/services/iia/exec") {
	        @Override
	        public Object handle(Request request, Response response) {
	        	
        		JSONObject data = new JSONObject();
        		try {
        			data = Controller.runIIA(request, response);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	JSONObject output = new JSONObject();
	        	try {
					output.put("status", true);
					output.put("data", data);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	           return output.toString();
	        }
	     });
		
		get(new Route("/services/test") {
	        @Override
	        public Object handle(Request request, Response response) {
	        
        		JSONObject data = new JSONObject();
        		try {
        			//data = ctl.runTest(request, response);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	        	
	        	JSONObject output = new JSONObject();
	        	try {
					output.put("status", true);
					output.put("data", data);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	           return output.toString();
	        }
	     });
	}
	
}