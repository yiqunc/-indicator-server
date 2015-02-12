package au.edu.csdila.indicator.isa;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.crs.ForceCoordinateSystemFeatureResults;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;

import au.edu.csdila.common.AppConfig;
import au.edu.csdila.common.MathUtils;
import au.edu.csdila.common.ShapeFileUtils;
import au.edu.csdila.common.WFSDataStore;
//import au.edu.csdila.common.connectivity.NetworkBufferBatch;
import au.edu.csdila.common.geoutils.*;
public class isa {
	
	static final Logger LOGGER = LoggerFactory.getLogger(isa.class);
	
	public static JSONObject exec(JSONObject options) throws Exception{
		
		int simId = options.getInt("simid");
		String simCode = options.getString("simcode");
		
		JSONObject output = new JSONObject();
		
		// for each element, the structure is: 
		/* [
		 * 				{
		 * 					layername:'hospital',
		 * 					layernameabbr:'HOS'
		 * 					},
		 * 				...
		 * ]
		 * 
		 * 
		 * */
		JSONArray infoArray = new JSONArray();
		
		String LGALayerName = AppConfig.getString("constantLAYERNAME_LGA"); 
		String SA1LayerName = AppConfig.getString("constantLAYERNAME_SA1");
		String FOILayerName = AppConfig.getString("constantLAYERNAME_FOI");
		String FOIPolygonLayerName = AppConfig.getString("constantLAYERNAME_FOI_POLYGON");
		String NetworkLayerName = AppConfig.getString("constantLAYERNAME_ROADNETWORK");
		
		//String TrainStationLayerName = AppConfig.getString("constantLAYERNAME_TRAINSTATION");
		//String TramStopLayerName = AppConfig.getString("constantLAYERNAME_TRAMSTOP");
		
		WFSDataStore wfsDS = new WFSDataStore();
		SimpleFeatureType schema = wfsDS.getDataStore().getSchema(LGALayerName);
		SimpleFeatureSource source = wfsDS.getFeatureSource(LGALayerName);
		Name geomNameLGA = schema.getGeometryDescriptor().getName();
		
		
		SimpleFeatureType schemaSA1 = wfsDS.getDataStore().getSchema(SA1LayerName);
		SimpleFeatureSource sourceSA1 = wfsDS.getFeatureSource(SA1LayerName);
		Name geomNameSA1 = schemaSA1.getGeometryDescriptor().getName();
		
		
		SimpleFeatureType schemaFOI = wfsDS.getDataStore().getSchema(FOILayerName);
		SimpleFeatureSource sourceFOI = wfsDS.getFeatureSource(FOILayerName);
		Name geomNameFOI = schemaFOI.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaFOIPolygon = wfsDS.getDataStore().getSchema(FOIPolygonLayerName);
		SimpleFeatureSource sourceFOIPolygon = wfsDS.getFeatureSource(FOIPolygonLayerName);
		Name geomNameFOIPolygon = schemaFOIPolygon.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaNetwork = wfsDS.getDataStore().getSchema(NetworkLayerName);
		SimpleFeatureSource sourceNetwork = wfsDS.getFeatureSource(NetworkLayerName);
		Name geomNameNetwork = schemaNetwork.getGeometryDescriptor().getName();
		
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		
		CoordinateReferenceSystem targetCRS = schema.getGeometryDescriptor().getCoordinateReferenceSystem();
	    
	    //projection 
	    Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
		CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
		CoordinateReferenceSystem CRS4326 = factory.createCoordinateReferenceSystem("EPSG:4326");
		CoordinateReferenceSystem CRS28355 = factory.createCoordinateReferenceSystem("EPSG:28355");
		
		
		// get input params
		
		JSONArray regionCodeList = options.getJSONArray("regionCodeList");
		JSONArray layerList = options.getJSONArray("layerList");
		String tmpOutputFolderPath = options.getString("tmpOutputFolderPath");
		
		double netbufSize = options.getDouble("netbufSize");
		double maxCatchmentRadius = 0.0;
		for(int i=0; i<layerList.length(); i++){
			if(layerList.getJSONObject(i).getDouble("catchmentradius") > maxCatchmentRadius){	
				maxCatchmentRadius = layerList.getJSONObject(i).getDouble("catchmentradius");
			}
		}
		
		LOGGER.info("==== maxCatchmentRadius :{}, netbufSize: {}", maxCatchmentRadius, netbufSize);

		// init infoArray 
		for(int i=0; i<layerList.length(); i++){
			JSONObject info = new JSONObject();
			info.put("layername", layerList.getJSONObject(i).getString("layername"));
			info.put("layernameabbr", layerList.getJSONObject(i).getString("layernameabbr"));
			infoArray.put(info);
		}
				
		/*the output shpfile path pattern is : 
		 * /tmpout/isa/username/jobcode_suc/jobcode_suc.shp
		 * /tmpout/isa/username/jobcode_isa/jobcode_isa.shp
		 * /tmpout/isa/username/jobcode_netbuf/jobcode_netbuf.shp
		*/
		
		//isa.shp is based on SA1 geom. for each SA1 and each type of services, an isa_servicetype column will be created 
		File f_isa = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_isa"));
		if(f_isa.exists()){FileUtils.deleteDirectory(f_isa);}	
		f_isa.mkdirs();
		String outputShpFilePath_isa = f_isa.getAbsolutePath()+File.separator+simCode+"_isa.shp";
		
		//suc.shp is based on LGA geom. for each LGA and each type of services, an suc_servicetype column will be created
		File f_suc = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_suc"));
		if(f_suc.exists()){FileUtils.deleteDirectory(f_suc);}	
		f_suc.mkdirs();
		String outputShpFilePath_suc = f_suc.getAbsolutePath()+File.separator+simCode+"_suc.shp";
		
		//netbuf.shp is created for each type of services, an servicetype column will be created to indicate which service type this buffer geom belongs to
		File f_netbuf = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_netbuf"));
		if(f_netbuf.exists()){FileUtils.deleteDirectory(f_netbuf);}	
		f_netbuf.mkdirs();
		String outputShpFilePath_netbuf = f_netbuf.getAbsolutePath()+File.separator+simCode+"_netbuf.shp";
		
		List<Filter> caFilterOrList = new ArrayList<Filter>();
		
		for(int i = 0; i<regionCodeList.length(); i++){
			caFilterOrList.add(ff.equals(ff.property(AppConfig.getString("constantATTRNAME_LGA_CODE")), ff.literal(regionCodeList.getInt(i))));
		}
		
		Filter caFilterOr = ff.or(caFilterOrList);

	    SimpleFeatureCollection rawfcLGA = source.getFeatures(caFilterOr);
	    DefaultFeatureCollection fcLGA =  new DefaultFeatureCollection();
	    SimpleFeatureIterator fIterator = rawfcLGA.features();
		 while (fIterator.hasNext()) 
	     {
			 fcLGA.add(fIterator.next());
		 }
		 fIterator.close();
	    
	    //////// prepare suc output shp file structure
		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
		stb.setName("suc");
		// define 'the_geom' as MultiPolygon for shp file
		stb.add("the_geom", MultiPolygon.class);
		stb.setDefaultGeometry("the_geom");
		for (AttributeDescriptor attDisc : schema.getAttributeDescriptors()) {
			String name = attDisc.getLocalName();
			Class type = attDisc.getType().getBinding();
			if (attDisc instanceof GeometryDescriptor) {
				// skip existing geometry field since we have created a new one 'the_geom' 
			} else {
				// try to keep all rest fields, since the field name cannot extend 10 chars, trim it if too long.
				stb.add(ShapeFileUtils.clapFieldName(name), type);
			}
		 }
		 // for each service layer, create a set of attributes
		 for(int i=0; i<layerList.length(); i++){
			 
			 //value of suc
			 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
			 stb.add("suc_"+strAbbr, Double.class);
			 stb.add("suc_"+strAbbr+"_r", Double.class);
			//number of ca optimum population
			 stb.add("optp_"+strAbbr, Double.class);
			 //number of ca serverd population
			 stb.add("srvp_"+strAbbr, Double.class);
			 //number of ca service points
			 stb.add("num_"+strAbbr, Integer.class);
			 
		 }
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType = stb.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC =  new DefaultFeatureCollection();
		 List<SimpleFeature> outputFL = new ArrayList<SimpleFeature>();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder = new SimpleFeatureBuilder(outputFeatureType);
		 
		/////// prepare isa output shp file structure
		SimpleFeatureTypeBuilder stb_isa = new SimpleFeatureTypeBuilder();
		stb_isa.setName("isa");
		// define 'the_geom' as MultiPolygon for shp file
		stb_isa.add("the_geom", MultiPolygon.class);
		stb_isa.setDefaultGeometry("the_geom");
		for (AttributeDescriptor attDisc : schemaSA1.getAttributeDescriptors()) {
			String name = attDisc.getLocalName();
			Class type = attDisc.getType().getBinding();
			if (attDisc instanceof GeometryDescriptor) {
				// skip existing geometry field since we have created a new one 'the_geom' 
			} else {
				// try to keep all rest fields, since the field name cannot extend 10 chars, trim it if too long.
				//LOGGER.info("attrname:{}", ShapeFileUtils.clapFieldAgePopName(name));
				stb_isa.add(ShapeFileUtils.clapFieldAgePopName(name), type);
			}
		 }
		 // for each service layer, create a set of attributes
		 for(int i=0; i<layerList.length(); i++){
			 
			 //value of suc of that ca
			 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
			 stb_isa.add("suc_"+strAbbr, Double.class);
			 //value of isa of each SA1
			 stb_isa.add("isa_"+strAbbr, Double.class);
			 stb_isa.add("isa_"+strAbbr+"_r", Double.class);
			 //value of slf of each SA1
			 stb_isa.add("slf_"+strAbbr, Double.class);
			 //number of target population of each SA1 for a specific service type
			 stb_isa.add("tarp_"+strAbbr, Double.class);
		 }

			 
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType_isa = stb_isa.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC_isa =  new DefaultFeatureCollection();
		 List<SimpleFeature> outputFL_isa = new ArrayList<SimpleFeature>();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder_isa = new SimpleFeatureBuilder(outputFeatureType_isa);
		 
		 
		/////// prepare netbuff output shp file structure
		 SimpleFeatureTypeBuilder stb_netbuf = new SimpleFeatureTypeBuilder();
		 
		 stb_netbuf.setName("netbuf");
		 stb_netbuf.add("the_geom", MultiPolygon.class);
		 stb_netbuf.setDefaultGeometry("the_geom");
		 stb_netbuf.add("servicetyp",String.class);
		 
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType_netbuf = stb_netbuf.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC_netbuf =  new DefaultFeatureCollection();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder_netbuf = new SimpleFeatureBuilder(outputFeatureType_netbuf);
	    
		 
		 //main loop (ca LGA level) starts here
		fIterator = fcLGA.features();
		while(fIterator.hasNext()){
			
		    //prepare caInfo;
		    JSONObject caInfo = new JSONObject();
			
			SimpleFeatureImpl feature =  (SimpleFeatureImpl)fIterator.next();
			//this the original polygon of lga
			Geometry polygon = (Geometry)feature.getDefaultGeometry();
			//need to create a buffered polygon of lga, it will be used to include more SA1s for the SUC calculation
			boolean lenient = true; // allow for some error due to different datums
		    MathTransform transform28355 = CRS.findMathTransform(CRS4326, CRS28355, lenient);
		    MathTransform transform4326 = CRS.findMathTransform(CRS28355, CRS4326, lenient);
		    //pointOfInterest is in 28355
		    Geometry polygon28355 = JTS.transform(polygon, transform28355);
			Geometry polygonBuffer28355 = polygon28355.buffer(maxCatchmentRadius + netbufSize);
			//then change buffer back to 4326
			Geometry polygonBuffer = JTS.transform(polygonBuffer28355, transform4326);
			
		    caInfo.put("caCode", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_CODE")));
		    caInfo.put("caName", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_NAME")).toString().trim());
		    
		    /////////////////////
		    
		    SimpleFeature outputf = outputFeatureBuilder.buildFeature(null);
			
		    // init output feature attributes with lga attributes, those appending suc attributes will be calculated later
			 for (AttributeDescriptor attDisc : schema.getAttributeDescriptors()) {
					String name = attDisc.getLocalName();
					// if current field is a geometry, we save it to the appointed field 'the_geom'
					if (attDisc instanceof GeometryDescriptor) {
						outputf.setAttribute("the_geom", feature.getAttribute(name));
					} else // otherwise, copy field value respectively
					{
						outputf.setAttribute(ShapeFileUtils.clapFieldName(name), feature.getAttribute(name));
					}
			 }
		    
			 //////////////////// step 0. calc sa1 within ca
			 
			 Filter filterSA1 = ff.intersects(ff.property(geomNameSA1), ff.literal(polygonBuffer));
			 //ATTENTION:　rawfcSA1 is (or 'will be') created from WFS query, which may not necessarily load everything in to memory (ref:http://docs.geotools.org/latest/javadocs/org/geotools/data/simple/SimpleFeatureCollection.html)
			 //this means when operations (e.g., call rawfcSA1.features()) applied on rawfcSA1, it may trigger the WFS query for multiple times, this can be annoying if that FeatureCollection will be used frequently. 
			 //a walkaround is to create a local copy of rawfcSA1 to contain/load all the features into memory. 
			 SimpleFeatureCollection rawfcSA1 = sourceSA1.getFeatures(filterSA1);
			 DefaultFeatureCollection fcSA1 =  new DefaultFeatureCollection();
			 SimpleFeatureIterator fIteratorSA1 = rawfcSA1.features();

			 while (fIteratorSA1.hasNext()) 
		     {
				 fcSA1.add(fIteratorSA1.next());
			 }
			 fIteratorSA1.close();
			 
			 
			 //////////////////// step 1. calc road network within ca
			 /*
			 Filter filterNetwork = ff.intersects(ff.property(geomNameNetwork), ff.literal(polygonBuffer));
			 SimpleFeatureCollection rawfcNetwork = sourceNetwork.getFeatures(filterNetwork);
			 
			 DefaultFeatureCollection fcNetwork =  new DefaultFeatureCollection();
			 SimpleFeatureIterator fIteratorNetwork = rawfcNetwork.features();

			 while (fIteratorNetwork.hasNext()) 
		     {
				 fcNetwork.add(fIteratorNetwork.next());
			 }
			 fIteratorNetwork.close();
			 
			 SimpleFeatureCollection fcNetwork28355 = new ForceCoordinateSystemFeatureResults(fcNetwork, CRS4326, false);
			 //the network CRS must be a projected one, e.g. EPSG:28355
			 fcNetwork28355 = new ReprojectingFeatureCollection(fcNetwork28355, CRS28355);
			 LOGGER.info("==== fcNetwork size:{}", fcNetwork28355.size());
			 */
			
			///////////////////// step 2. sub loop for each service type
			for(int i=0; i<layerList.length(); i++){
				
				String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
				// one layer main comprise more than one featsubtyps, join them by OR
				JSONArray featsubtyps = layerList.getJSONObject(i).getJSONArray("featsubtyps");
				List<Filter> foiFilterOrList = new ArrayList<Filter>();
				for(int j=0; j<featsubtyps.length();j++)
				{
					foiFilterOrList.add(ff.equals(ff.property(AppConfig.getString("constantLAYERNAME_FOI_FEATSUBTYP")), ff.literal(featsubtyps.getString(j))));
				}
				
				Filter foiFilterOr = ff.or(foiFilterOrList);
				Filter filterFOI = null;
				SimpleFeatureCollection rawfcFOI = null;
				//handle differently for 'park' 
				if(layerList.getJSONObject(i).getString("layername").equalsIgnoreCase("park")){
					
					filterFOI = ff.and(foiFilterOr, ff.intersects(ff.property(geomNameFOIPolygon), ff.literal(polygon)));
					rawfcFOI = sourceFOIPolygon.getFeatures(filterFOI);
				}
				else{
					
					filterFOI = ff.and(foiFilterOr, ff.intersects(ff.property(geomNameFOI), ff.literal(polygon)));
					rawfcFOI = sourceFOI.getFeatures(filterFOI);
				}
				
				
				SimpleFeatureCollection fcFOI = DataUtilities.collection(rawfcFOI);
				 
				
				//assign attribute values to outputf 
				outputf.setAttribute("optp_"+strAbbr, layerList.getJSONObject(i).getInt("optipop"));
				outputf.setAttribute("num_"+strAbbr, fcFOI.size());
				
				//if this type of service doesn't exist in currrent ca, set ca's suc to 0.
				if(fcFOI.size() == 0)
				{
					outputf.setAttribute("suc_"+strAbbr, 0);
					outputf.setAttribute("srvp_"+strAbbr, 0);
				}else{
					
					SimpleFeatureCollection fcFOI28355 = new ForceCoordinateSystemFeatureResults(fcFOI, CRS4326, false);
					//the FOI CRS must be a projected one, e.g. EPSG:28355
					fcFOI28355 = new ReprojectingFeatureCollection(fcFOI28355, CRS28355);
					
					// calc target population of that service
					// substep 1. calc network buffer 
					SimpleFeatureCollection intersectedSA1 = null;
					SimpleFeatureCollection fcNB = null;
					if(layerList.getJSONObject(i).getString("layername").equalsIgnoreCase("park")){
						
						//directly create a buffer (using 'catchmentradius') around each park geom and intersect it with SA1s
						SimpleFeatureIterator tmpFOIPolygonIterator = fcFOI28355.features();
						DefaultFeatureCollection tmpfcFOIPolygonBuffer=  new DefaultFeatureCollection();

						while (tmpFOIPolygonIterator.hasNext()) 
					    {
							SimpleFeature fPolygon = tmpFOIPolygonIterator.next();
							Geometry poly = (Geometry)fPolygon.getDefaultGeometry();
							Geometry polyBuffer = poly.buffer(Double.parseDouble(layerList.getJSONObject(i).getString("catchmentradius")));
							fPolygon.setAttribute("wkb_geometry", polyBuffer);
							tmpfcFOIPolygonBuffer.add(fPolygon);
						}
						tmpFOIPolygonIterator.close();
						
						fcNB = new ForceCoordinateSystemFeatureResults(tmpfcFOIPolygonBuffer, CRS28355, false);
						fcNB = new ReprojectingFeatureCollection(fcNB, CRS4326);
					}
					else{
						//if not PARK, using NetworkBuffer to create buffers, otherwize, directly create buffer over park polygon geom (TBD)
						NumberFormat formatter = new DecimalFormat("#0.00000");
						//Gus's network buffer method
						long start = System.currentTimeMillis();
						au.edu.csdila.common.connectivity.NetworkBufferBatch nbb = new au.edu.csdila.common.connectivity.NetworkBufferBatch(sourceNetwork, fcFOI, layerList.getJSONObject(i).getDouble("catchmentradius"), netbufSize);
						SimpleFeatureCollection fcNB28355 = nbb.createBuffers();
						fcNB = new ForceCoordinateSystemFeatureResults(fcNB28355, CRS28355, false);
						fcNB = new ReprojectingFeatureCollection(fcNB, CRS4326);
						long end = System.currentTimeMillis();
						LOGGER.info("==== method 1 Execution time is:{} seconds", formatter.format((end - start) / 1000d));
						
						/*
						//Benny's new network method
						
						NumberFormat formatter = new DecimalFormat("#0.00000");
						long start = System.currentTimeMillis();
						NetworkBufferBatch nbb = new NetworkBufferBatch(fcNetwork28355, fcFOI28355, layerList.getJSONObject(i).getDouble("catchmentradius"), netbufSize);
						SimpleFeatureCollection fcNB28355 = nbb.createBuffers();
					    fcNB = new ForceCoordinateSystemFeatureResults(fcNB28355, CRS28355, false);
						fcNB = new ReprojectingFeatureCollection(fcNB, CRS4326);
						long end = System.currentTimeMillis();
						LOGGER.info("====method Execution time is:{} seconds", formatter.format((end - start) / 1000d));
						*/
					}
						// substep 2. intersect network buffer with SA1 using STR tree, remove all duplicated SA1s 
						SimpleFeatureIterator iteratorNB = fcNB.features();
						
						LOGGER.info("==== fcNB size:{}", fcNB.size());
						
						while (iteratorNB.hasNext()) 
				        {
							SimpleFeature outputf_netbuf = outputFeatureBuilder_netbuf.buildFeature(null);
							
							SimpleFeatureImpl fNB =  (SimpleFeatureImpl)iteratorNB.next();
							Geometry gNB = (Geometry) fNB.getDefaultGeometry();
				            if (!gNB.isValid()) {
				                    // skip bad data
				                	continue;
				            }
				            
				            outputf_netbuf.setAttribute("the_geom", fNB.getDefaultGeometry());
				            outputf_netbuf.setAttribute("servicetyp", strAbbr);
				            outputFC_netbuf.add(outputf_netbuf);

				        }
						iteratorNB.close();

						intersectedSA1 = GeometryUtils.intersection(fcSA1, fcNB);
					
					
					LOGGER.info("===== unique SA1 num: {}", intersectedSA1.size());
					
					// substep 3. calc total served population of all instersected SA1s

					double servPop = calcTargetPopulation(intersectedSA1, "all", "all");
					
					
					// substep 4. calc suc for ca
					if(servPop < 0.001){
						outputf.setAttribute("suc_"+strAbbr, 0);
						outputf.setAttribute("srvp_"+strAbbr, 0);
					}else
					{
						outputf.setAttribute("suc_"+strAbbr, layerList.getJSONObject(i).getInt("optipop")*fcFOI.size()/servPop);
						outputf.setAttribute("srvp_"+strAbbr, servPop);
					}
					
				}
				
			}
				
			///////////////////// step 3. calc SLF/ISA for each sa1 for each service type
			//since currently the fcSA1 is based on buffered LGA boundary, need to trim it with exact LGA boundary
			SimpleFeatureCollection fcSA1Trimed = GeometryUtils.intersection(fcSA1, polygon, 0.8); 
			fIteratorSA1 = fcSA1Trimed.features();
			 while (fIteratorSA1.hasNext()) 
		     {				
				 SimpleFeature outputf_isa = outputFeatureBuilder_isa.buildFeature(null);
				 
				 SimpleFeatureImpl f =  (SimpleFeatureImpl)fIteratorSA1.next();
				// init output feature attributes with sa1 attributes, those appending isa attributes will be calculated later
				 for (AttributeDescriptor attDisc : schemaSA1.getAttributeDescriptors()) {
						String name = attDisc.getLocalName();
						// if current field is a geometry, we save it to the appointed field 'the_geom'
						if (attDisc instanceof GeometryDescriptor) {
							outputf_isa.setAttribute("the_geom", f.getAttribute(name));
						} else {// otherwise, copy field value respectively
						
							outputf_isa.setAttribute(ShapeFileUtils.clapFieldAgePopName(name), f.getAttribute(name));
							//LOGGER.info("attrname:{} value:{}", ShapeFileUtils.clapFieldAgePopName(name), outputf_isa.getAttribute(ShapeFileUtils.clapFieldAgePopName(name)));
						}
				 }
				 
				for(int i=0; i<layerList.length(); i++)
				{
					 String mages = layerList.getJSONObject(i).getString("mages");
					 String fages = layerList.getJSONObject(i).getString("fages");
					
					 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
					 outputf_isa.setAttribute("suc_"+strAbbr, Double.parseDouble(outputf.getAttribute("suc_"+strAbbr).toString()));
					 
					 double ttlpop = Double.parseDouble(outputf_isa.getAttribute(AppConfig.getString("constantLAYERNAME_SA1_TOTAL_POP")).toString());
					 if(ttlpop<0.01){
						 outputf_isa.setAttribute("tarp_"+strAbbr, 0.0);
						 outputf_isa.setAttribute("slf_"+strAbbr, 0.0);
						 outputf_isa.setAttribute("isa_"+strAbbr, 0.0);
					 }else
					 {
						 outputf_isa.setAttribute("tarp_"+strAbbr, calcTargetPopulation(outputf_isa, mages, fages));
						 outputf_isa.setAttribute("slf_"+strAbbr, Double.parseDouble(outputf_isa.getAttribute("tarp_"+strAbbr).toString()) / ttlpop);
						 outputf_isa.setAttribute("isa_"+strAbbr, Double.parseDouble(outputf_isa.getAttribute("suc_"+strAbbr).toString()) * Double.parseDouble(outputf_isa.getAttribute("slf_"+strAbbr).toString()));
					 }
				}
				
				//outputFC_isa.add(outputf_isa);
				outputFL_isa.add(outputf_isa);
				 
			 }
			 fIteratorSA1.close();

			//outputFC.add(outputf);
			 outputFL.add(outputf);

		}
		
		fIterator.close();
		wfsDS.dispose();

		///////////////////// export shpfile
		//sort suc
		for(int k=0; k<layerList.length(); k++){
			
			String strAbbr = layerList.getJSONObject(k).getString("layernameabbr");
			final String strColName = "suc_"+strAbbr;
			Collections.sort(outputFL, new Comparator<SimpleFeature>() {
			    @Override
			    public int compare(SimpleFeature a, SimpleFeature b) {
			            if((double)a.getAttribute(strColName) < (double)b.getAttribute(strColName)){
			            	return 1;
			            }else if((double)a.getAttribute(strColName) == (double)b.getAttribute(strColName)){
			            	return 0;
			            }
			            else{
			            	return -1;
			            }
			    }
			});
	
			int rank = 1;
			int jump = 0;
			for(int i=0; i<outputFL.size();i++){
				outputFL.get(i).setAttribute(strColName+"_r", rank);
				
				//if not the last element
				if(i < outputFL.size() -1)
				{
					//only increase the rank if current value is NOT equal to the next value
					if(!MathUtils.doubleEquals((double)outputFL.get(i).getAttribute(strColName), (double)outputFL.get(i+1).getAttribute(strColName), 0.000000001))
					{
						rank = rank + jump +1;
						jump = 0;
					}else
					{
						jump++;
					}
				}
			}
		}
		
		//all sorting and ranking done, put it featureCollection
		for(int i=0; i<outputFL.size();i++){
			outputFC.add(outputFL.get(i));
		}
		
		
		//sort isa
		for(int k=0; k<layerList.length(); k++){
			
			String strAbbr = layerList.getJSONObject(k).getString("layernameabbr");
			final String strColName = "isa_"+strAbbr;
			Collections.sort(outputFL_isa, new Comparator<SimpleFeature>() {
			    @Override
			    public int compare(SimpleFeature a, SimpleFeature b) {
			            if((double)a.getAttribute(strColName) < (double)b.getAttribute(strColName)){
			            	return 1;
			            }else if((double)a.getAttribute(strColName) == (double)b.getAttribute(strColName)){
			            	return 0;
			            }
			            else{
			            	return -1;
			            }
			    }
			});
	
			int rank = 1;
			int jump = 0;
			for(int i=0; i<outputFL_isa.size();i++){
				outputFL_isa.get(i).setAttribute(strColName+"_r", rank);

				//if not the last element
				if(i < outputFL_isa.size() -1)
				{
					//only increase the rank if current value is NOT equal to the next value
					if(!MathUtils.doubleEquals((double)outputFL_isa.get(i).getAttribute(strColName), (double)outputFL_isa.get(i+1).getAttribute(strColName), 0.000000001))
					{
						rank = rank + jump +1;
						jump = 0;
					}else
					{
						jump++;
					}
				}
			}
		}
		
		//all sorting and ranking done, put it featureCollection
		for(int i=0; i<outputFL_isa.size();i++){
			outputFC_isa.add(outputFL_isa.get(i));
		}
				
	
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType, outputFC, new File(outputShpFilePath_suc), true, CRS.decode("EPSG:4326"));
		
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType_isa, outputFC_isa, new File(outputShpFilePath_isa), true, CRS.decode("EPSG:4326"));
		
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType_netbuf, outputFC_netbuf, new File(outputShpFilePath_netbuf), true, CRS.decode("EPSG:4326"));

		///////////////////// prepare data for geoserver publishing
		JSONArray vectors = new JSONArray();
		// add suc layer
		JSONObject vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:4326");
		vec.put("path", outputShpFilePath_suc);
		vec.put("name", simCode+"_suc");
		vec.put("layertype", "polygon");
		vec.put("layerclass", "suc");
		vectors.put(vec);
		
		// add isa layer
	    vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:4326");
		vec.put("path", outputShpFilePath_isa);
		vec.put("name", simCode+"_isa");
		vec.put("layertype", "polygon");
		vec.put("layerclass", "isa");
		vectors.put(vec);
		
		// add netbuf layer
	    vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:4326");
		vec.put("path", outputShpFilePath_netbuf);
		vec.put("name", simCode+"_netbuf");
		vec.put("layertype", "polygon");
		vec.put("layerclass", "netbuf");
		vectors.put(vec);
		
		output.put("infos", infoArray);
		output.put("vectors", vectors);
		
		return output;
	}
	
	/***
	 * 
	 * @param fcSA1
	 * @param mages
	 * @param fages
	 * @return
	 */
	public static double calcTargetPopulation(SimpleFeatureCollection fcSA1, String mages, String fages) {
		
		double result = 0.0;
	    
		ArrayList<String> ageColNameArr = getTargetPopulationCols(mages, fages, "");

		SimpleFeatureIterator fIteratorSA1  = fcSA1.features();
		while(fIteratorSA1.hasNext()){
			SimpleFeature fSA1 = fIteratorSA1.next();
			for(int i=0; i< ageColNameArr.size(); i++){
				result =  result+ Double.parseDouble(fSA1.getAttribute(ageColNameArr.get(i)).toString());
			}
		}
		fIteratorSA1.close();
		return result;
		
	}
	
	
	/**
	 * get target population of a SA1 based on male and female age group definition
	 * @param fSA1
	 * @param mages
	 * @param fages
	 * @return
	 */
	public static double calcTargetPopulation(SimpleFeature fSA1, String mages, String fages) {
		
		double result = 0.0;
		
		ArrayList<String> ageColNameArr = getTargetPopulationCols(mages, fages, "age_yr_");

		for(int i=0; i< ageColNameArr.size(); i++){
			result =  result+ Double.parseDouble(fSA1.getAttribute(ageColNameArr.get(i)).toString());
		}
		return result;
		
	}
	
	/**
	 * 
	 * @param mages
	 * @param fages
	 * @param removeColPrefix
	 * @return
	 */
	public static ArrayList<String> getTargetPopulationCols(String mages, String fages, String removeColPrefix){
		
		ArrayList<String> ageColNameArr = new ArrayList<String>();
		
		if(mages.equalsIgnoreCase("all"))
		{
			ageColNameArr.add("tot_m");
		}
		if(fages.equalsIgnoreCase("all"))
		{
			ageColNameArr.add("tot_f");
		}
		
		if(mages.indexOf("-") > 0){
			String colNamePattern = AppConfig.getString("constantLAYERNAME_SA1_POP_PATTERN");
			colNamePattern = colNamePattern.replaceAll(removeColPrefix, "");
			colNamePattern = colNamePattern.replaceAll("SEX", "m");
			String[] agebound = mages.split("-");
			int lowBound = Integer.parseInt(agebound[0]);
			int highBound = Integer.parseInt(agebound[1]);
			for(int i=lowBound; i<=highBound; i++){
				String ageColName = colNamePattern.replaceAll("YEAR", String.valueOf(i));
				ageColNameArr.add(ageColName);
			}
		}
		
		if(fages.indexOf("-") > 0){
			String colNamePattern = AppConfig.getString("constantLAYERNAME_SA1_POP_PATTERN");
			colNamePattern = colNamePattern.replaceAll(removeColPrefix, "");
			colNamePattern = colNamePattern.replaceAll("SEX", "f");
			String[] agebound = fages.split("-");
			int lowBound = Integer.parseInt(agebound[0]);
			int highBound = Integer.parseInt(agebound[1]);
			for(int i=lowBound; i<=highBound; i++){
				String ageColName = colNamePattern.replaceAll("YEAR", String.valueOf(i));
				ageColNameArr.add(ageColName);
			}
		}
		
		return ageColNameArr;
	}

}
