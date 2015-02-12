package au.edu.csdila.indicator.iop;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.math.BigDecimal;

import org.apache.commons.io.FileUtils;
import org.geotools.data.DataSourceException;
import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.crs.ForceCoordinateSystemFeatureResults;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.GeometryBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.path.Path;
import org.geotools.graph.structure.Edge;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.geometry.BoundingBox;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequences;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import au.edu.csdila.common.AppConfig;
import au.edu.csdila.common.MathUtils;
import au.edu.csdila.common.ShapeFileUtils;
import au.edu.csdila.common.WFSDataStore;

import au.edu.csdila.indicator.*;

public class iop {
	
	static final Logger LOGGER = LoggerFactory.getLogger(iop.class);
	
	public static JSONObject exec(JSONObject options) throws DataSourceException, IOException, NoSuchAuthorityCodeException, FactoryException, SchemaException, TransformException, JSONException{
		
		int simId = options.getInt("simid");
		String simCode = options.getString("simcode");
		
		JSONObject output = new JSONObject();
		
		// this variable contains info for calculate iop for each ca. 
		// for each element, the structure is: 
		/* {
		 * 		caCode:341,
		 * 		caName:'Melbourne',
		 * 		caCardinalPoints:[[x,y],[x,y]...],
		 * 		caPerimeter:12222,
		 * 		caFois:[
		 * 				{
		 * 					featsubtyps:['hospital'],
		 * 					abbr:'HOS',
		 * 					points:[[x,y],[x,y]...],
		 * 					meanDistance: 1238,
		 * 					indexValue: 0.8
		 * 				},
		 * 				...
		 * 				]
		 * }
		 * 
		 * 
		 * */
		JSONArray infoArray = new JSONArray();
		
		String LGALayerName = AppConfig.getString("constantLAYERNAME_LGA"); 
		String MBLayerName = AppConfig.getString("constantLAYERNAME_MESHBLOCK");
		String FOILayerName = AppConfig.getString("constantLAYERNAME_FOI");
		String MBCatName = AppConfig.getString("constantATTRNAME_MESHBLOCK_CAT_NAME");
		String MBPopulationName = AppConfig.getString("constantATTRNAME_MESHBLOCK_POPULATION_NAME");
		String MBCatValueResidential = AppConfig.getString("constantATTRNAME_MESHBLOCK_CAT_VALUE_RESIDENTIAL");
		String TrainStationLayerName = AppConfig.getString("constantLAYERNAME_TRAINSTATION");
		String TramStopLayerName = AppConfig.getString("constantLAYERNAME_TRAMSTOP");
		
		WFSDataStore wfsDS = new WFSDataStore();
		SimpleFeatureType schema = wfsDS.getDataStore().getSchema(LGALayerName);
		SimpleFeatureSource source = wfsDS.getFeatureSource(LGALayerName);
		Name geomNameLGA = schema.getGeometryDescriptor().getName();
		
		
		SimpleFeatureType schemaMB = wfsDS.getDataStore().getSchema(MBLayerName);
		SimpleFeatureSource sourceMB = wfsDS.getFeatureSource(MBLayerName);
		Name geomNameMB = schemaMB.getGeometryDescriptor().getName();
		
		
		SimpleFeatureType schemaFOI = wfsDS.getDataStore().getSchema(FOILayerName);
		SimpleFeatureSource sourceFOI = wfsDS.getFeatureSource(FOILayerName);
		Name geomNameFOI = schemaFOI.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaTrain = wfsDS.getDataStore().getSchema(TrainStationLayerName);
		SimpleFeatureSource sourceTrain = wfsDS.getFeatureSource(TrainStationLayerName);
		Name geomNameTrain = schemaTrain.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaTram = wfsDS.getDataStore().getSchema(TramStopLayerName);
		SimpleFeatureSource sourceTram = wfsDS.getFeatureSource(TramStopLayerName);
		Name geomNameTram = schemaTram.getGeometryDescriptor().getName();
		
		
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		
		CoordinateReferenceSystem targetCRS = schema.getGeometryDescriptor().getCoordinateReferenceSystem();
	    
	    //projection 
	    Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
		CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
		CoordinateReferenceSystem CRS4326 = factory.createCoordinateReferenceSystem("EPSG:4326");
		CoordinateReferenceSystem CRS28355 = factory.createCoordinateReferenceSystem("EPSG:28355");
		
	    boolean lenient = true; // allow for some error due to different datums
	    MathTransform transform28355 = CRS.findMathTransform(CRS4326, CRS28355, lenient);
	    MathTransform transform4326 = CRS.findMathTransform(CRS28355, CRS4326, lenient);
	    
		// get input params
		
		JSONArray regionCodeList = options.getJSONArray("regionCodeList");
		int iopCtrlPointNum = options.getInt("iopCtrlPointNum");
		// add a buffer around LGA so that more facilities can be included for calculation
		double iopBufferDist = options.getDouble("iopBufferDist");
		JSONArray layerList = options.getJSONArray("layerList");
		
		String tmpOutputFolderPath = options.getString("tmpOutputFolderPath");
		
		/*the output shpfile path pattern is : 
		 * /tmpout/iop/username/jobcode_iop/jobcode_iop.shp
		 * /tmpout/iop/username/jobcode_iop_cardinalpoint/jobcode_iop_cardinalpoint.shp
		*/
		
		
		File f_iop = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_iop"));
		if(f_iop.exists()){FileUtils.deleteDirectory(f_iop);}	
		f_iop.mkdirs();
		String outputShpFilePath_iop = f_iop.getAbsolutePath()+File.separator+simCode+"_iop.shp";
		
		
		File f_iop_cardinalpoint = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_iop_cardinalpoint"));
		if(f_iop_cardinalpoint.exists()){FileUtils.deleteDirectory(f_iop_cardinalpoint);}	
		f_iop_cardinalpoint.mkdirs();
		String outputShpFilePath_iop_cardinalpoint = f_iop_cardinalpoint.getAbsolutePath()+File.separator+simCode+"_iop_cardinalpoint.shp";
		
		List<Filter> caFilterOrList = new ArrayList<Filter>();
		
		for(int i = 0; i<regionCodeList.length(); i++){
			caFilterOrList.add(ff.equals(ff.property(AppConfig.getString("constantATTRNAME_LGA_CODE")), ff.literal(regionCodeList.getInt(i))));
		}
		
		Filter caFilterOr = ff.or(caFilterOrList);
			  
	    SimpleFeatureCollection fc = source.getFeatures(caFilterOr);

	    SimpleFeatureIterator fIteratorOriginal = fc.features();
		
	    //very important to force fc into CRS4326 before doing the reprojecting to CRS28355. otherwise, odd coords will be generated.
	    //ref: http://www.massapi.com/source/geoserver-2.1.1-src/geoserver-2.1.1/extension/wps/wps-core/src/main/java/org/geoserver/wps/gs/ReprojectProcess.java.html
	    SimpleFeatureCollection fc28355 = new ForceCoordinateSystemFeatureResults(fc, CRS4326, false);
	    fc28355 = new ReprojectingFeatureCollection(fc28355, CRS28355);
	    
	    SimpleFeatureIterator fIterator = fc28355.features();
	    
	    // prepare iop output shp file structure
		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
		stb.setName("iop");
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
		 // for each iopLayer, create a set of attributes
		 for(int i=0; i<layerList.length(); i++){
			 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
			 //value of iop 
			 stb.add("iop_"+strAbbr, Double.class);
			 //rank of iop
			 stb.add("iop_"+strAbbr+"_r", Double.class);
			 //number of ca Cardinal Points
			 //stb.add("pn_"+strAbbr, Integer.class);
			 //number of ca Facility Points
			 stb.add("num_"+strAbbr, Integer.class);
			 
		 }
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType = stb.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC =  new DefaultFeatureCollection();
		 List<SimpleFeature> outputFL = new ArrayList<SimpleFeature>();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder = new SimpleFeatureBuilder(outputFeatureType);
		 
		// prepare iop_cardinalpoint output shp file structure
		SimpleFeatureTypeBuilder stb_cardinalpoint = new SimpleFeatureTypeBuilder();
		stb_cardinalpoint.setName("iop_cardinalpoint");
		// define 'the_geom' as MultiPolygon for shp file
		stb_cardinalpoint.add("the_geom", MultiPoint.class);
		// for each iopLayer, create a set of attributes
		 
		stb_cardinalpoint.add("caCode",String.class);
		stb_cardinalpoint.add("caName",String.class);
		stb_cardinalpoint.add("jobId", Integer.class);
		stb_cardinalpoint.add("ptNum", Integer.class);
			 
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType_cardinalpoint = stb_cardinalpoint.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC_cardinalpoint =  new DefaultFeatureCollection();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder_cardinalpoint = new SimpleFeatureBuilder(outputFeatureType_cardinalpoint);
	    
		while(fIteratorOriginal.hasNext() && fIterator.hasNext()){
			
		    //prepare caInfo;
		    JSONObject caInfo = new JSONObject();
			JSONArray caCardinalPoints = new JSONArray();
			JSONArray caFois = new JSONArray();
			
			SimpleFeatureImpl feature =  (SimpleFeatureImpl)fIteratorOriginal.next();
			BoundingBox caBbox = feature.getBounds();
			Geometry polygon = (Geometry)feature.getDefaultGeometry();
			//the default geom is in 4326, need to convert to 28355 to create the buffer then convert it back. Annoying!
			Geometry polygon28355 = JTS.transform(polygon, transform28355);
			Geometry polygonBuffer28355 = polygon28355.buffer(iopBufferDist);
			Geometry polygonBuffer = JTS.transform(polygonBuffer28355, transform4326);
			
		    caInfo.put("caCode", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_CODE")));
		    caInfo.put("caName", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_NAME")).toString().trim());
		    
		    /////////////////////
		    
		    SimpleFeature outputf = outputFeatureBuilder.buildFeature(null);
			 
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
		    
			
			///////////////////// step 0. calc fois within buffered ca boundary
			 
			 for(int i=0; i<layerList.length(); i++){
				 
				 	JSONObject foi = new JSONObject();
					foi.put("featsubtyps", layerList.getJSONObject(i).getJSONArray("featsubtyps"));
					foi.put("abbr", layerList.getJSONObject(i).getString("layernameabbr"));
					foi.put("points", new JSONArray("[]"));
					foi.put("meanDistance", 0.0);
					foi.put("indexValue", 0.0);

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
					//handle differently for 'trainstation' and 'tramstop'
					if(layerList.getJSONObject(i).getString("layername").equalsIgnoreCase("trainstation")){
						
						rawfcFOI = sourceTrain.getFeatures(ff.intersects(ff.property(geomNameTrain), ff.literal(polygonBuffer)));
						
					}else if(layerList.getJSONObject(i).getString("layername").equalsIgnoreCase("tramstop")){
						
						rawfcFOI = sourceTram.getFeatures(ff.intersects(ff.property(geomNameTram), ff.literal(polygonBuffer)));
					}
					else{
						
						filterFOI = ff.and(foiFilterOr, ff.intersects(ff.property(geomNameFOI), ff.literal(polygonBuffer)));
						rawfcFOI = sourceFOI.getFeatures(filterFOI);
					}
					SimpleFeatureCollection fcFOI = DataUtilities.collection(rawfcFOI);
					
					SimpleFeatureCollection fcFOI28355 = new ForceCoordinateSystemFeatureResults(fcFOI, CRS4326, false);
					fcFOI28355 = new ReprojectingFeatureCollection(fcFOI28355, CRS28355);
					
					SimpleFeatureIterator fIteratorFOI = fcFOI28355.features();
					
					while(fIteratorFOI.hasNext()){
						SimpleFeatureImpl f =  (SimpleFeatureImpl)fIteratorFOI.next();
						Geometry g = (Geometry)f.getDefaultGeometry();
						foi.getJSONArray("points").put(new JSONArray ("["+g.getCoordinate().x+","+g.getCoordinate().y+"]"));
						
					}
					fIteratorFOI.close();
					
					caFois.put(foi);
			 }
			
			///////////////////// step 1. calc pop weighted centroid for each ca
			
			Filter filterMB = ff.and(ff.equals(ff.property(MBCatName), ff.literal(MBCatValueResidential)), ff.intersects(ff.property(geomNameMB), ff.literal(polygon)));	 
			SimpleFeatureCollection fcMB = sourceMB.getFeatures(filterMB);
			
		    //System.out.println("====== :"+fcMB.size()+" MB in "+feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_NAME")));
		    
		    //do projection
		    SimpleFeatureCollection fcMB28355 = new ForceCoordinateSystemFeatureResults(fcMB, CRS4326, false);
		    fcMB28355 = new ReprojectingFeatureCollection(fcMB28355, CRS28355);
		    
		    SimpleFeatureIterator fIteratorMB = fcMB28355.features();
		    
		    double totalPopulation = 0;
		    double centroid_x = 0.0;
		    double centroid_y = 0.0;
		    
		    while(fIteratorMB.hasNext()){
				SimpleFeatureImpl f =  (SimpleFeatureImpl)fIteratorMB.next();
				Geometry mb = (Geometry)f.getDefaultGeometry();
				
				Coordinate centroid = mb.getCentroid().getCoordinate();
				double pop = ((BigDecimal)f.getAttribute(MBPopulationName)).doubleValue();
				totalPopulation = totalPopulation + pop;
				centroid_x = centroid_x + centroid.x * pop;
				centroid_y = centroid_y + centroid.y * pop;
		    }
		    fIteratorMB.close();
		    
		    if(totalPopulation>0){
		    	
		    	centroid_x = centroid_x/totalPopulation;
		    	centroid_y = centroid_y/totalPopulation;
		    }
		    
		    //System.out.println("======= mean centroid (internal cp): "+ centroid_x + " , "+centroid_y);
		    
		    
		    caCardinalPoints.put(new JSONArray ("["+centroid_x+","+centroid_y+"]"));
		    
		    
		    // step 2. calc cardinal points for each ca
		    feature =  (SimpleFeatureImpl)fIterator.next();
			polygon = (Geometry)feature.getDefaultGeometry();
			
			//System.out.println("====== length :"+polygon.getLength());
			caInfo.put("caPerimeter", polygon.getLength());
			
			int cardinalPointNum = iopCtrlPointNum;
			double segLengthThreshold = polygon.getLength()/cardinalPointNum;
			
			int startPointIdx = 0;
			
			Coordinate[] corrArr = polygon.getCoordinates();
			List<Coordinate> cardinalPointsArr = new ArrayList<Coordinate>();
			
			//add the start point as the first cardinal point
			//cardinalPointsArr.add(corrArr[startPointIdx]);
			//System.out.println("======= cp["+ (cardinalPointsArr.size()-1) +"] - "+ corrArr[startPointIdx].x + " , "+corrArr[startPointIdx].y);
			double accumulatedSegLength = 0;
			double ttlSegLength = 0;
			//System.out.println("====== number of points :"+corrArr.length);
			for(int i=0; i<corrArr.length; i++){
				if(startPointIdx==corrArr.length-1) startPointIdx = 0;
				
				//System.out.println(i+" - "+ corrArr[startPointIdx].x + " , "+corrArr[startPointIdx].y);
				startPointIdx++;
				double diffx = corrArr[startPointIdx].x-corrArr[startPointIdx-1].x;
				double diffy = corrArr[startPointIdx].y-corrArr[startPointIdx-1].y;
			
				double stepLength = Math.sqrt(diffx*diffx + diffy*diffy);
				ttlSegLength = ttlSegLength + stepLength;
				accumulatedSegLength = accumulatedSegLength + stepLength;
				
				if (accumulatedSegLength > segLengthThreshold){
					
					double diffSegLength = accumulatedSegLength - segLengthThreshold;
					
					double x_cp = corrArr[startPointIdx].x - (diffSegLength/stepLength) * diffx;
					double y_cp = corrArr[startPointIdx].y - (diffSegLength/stepLength) * diffy;
					
					cardinalPointsArr.add(new Coordinate(x_cp, y_cp));
					//System.out.println("======= cp["+ (cardinalPointsArr.size()-1) +"] - "+ x_cp + " , "+y_cp);
					
					caCardinalPoints.put(new JSONArray ("["+x_cp+","+y_cp+"]"));
					accumulatedSegLength = diffSegLength;
				}
			}
			
			///////////////////// step2.5. prepare road network for current ca
			JSONArray allodpoints = new JSONArray();
			//put caCardinalPoints into allodpoints
			for(int i = 0; i < caCardinalPoints.length(); i++){
				allodpoints.put(caCardinalPoints.getJSONArray(i));
			}
			//put points of each foi type into allodpoints
			for(int i = 0; i<caFois.length(); i++){
				
				for(int j = 0; j < caFois.getJSONObject(i).getJSONArray("points").length(); j++){
					allodpoints.put(caFois.getJSONObject(i).getJSONArray("points").getJSONArray(j));
				}
			}
			
			LineStringGraphGenerator lineStringGraphGenerator = calcRoadNetwork(allodpoints, caBbox);
			
			///////////////////// step3. calc mean OD distance for each type of FOI for each ca
			for(int i = 0; i<caFois.length(); i++){
				
				String strAbbr = caFois.getJSONObject(i).get("abbr").toString();
				
				if(caFois.getJSONObject(i).getJSONArray("points").length() > 0){
					
					double meanDistance  = calcMeanDistance(caCardinalPoints, caFois.getJSONObject(i).getJSONArray("points"), lineStringGraphGenerator);
					
					//based on Ian's suggestion, directly using mean network distance as the index value
					//double indexValue = meanDistance / (caInfo.getDouble("caPerimeter") * caFois.getJSONObject(i).getJSONArray("points").length());
					double indexValue = meanDistance;
				
					caFois.getJSONObject(i).put("meanDistance", meanDistance);
					caFois.getJSONObject(i).put("indexValue", indexValue);
					
				} else {
					
					caFois.getJSONObject(i).put("meanDistance", -1);
					caFois.getJSONObject(i).put("indexValue", -1);
				}
				
				// attach result to new output shape file. 
				outputf.setAttribute("iop_"+strAbbr,  caFois.getJSONObject(i).get("indexValue"));
				//outputf.setAttribute("pn_"+strAbbr,  caCardinalPoints.length());
				outputf.setAttribute("num_"+strAbbr,  caFois.getJSONObject(i).getJSONArray("points").length());
			}
			

			caInfo.put("caCardinalPoints", caCardinalPoints);
			caInfo.put("caFois", caFois);
			
			///////////////////// step4, add caInfo into infoArray
			infoArray.put(caInfo);
			
			///////////////////// step5, attach caInfo into new output shape file. 
			//outputFC.add(outputf);
			outputFL.add(outputf);
			
			///////////////////// step6, build cardinalpoint for output shape file. 
			SimpleFeature outputf_cardinalpoint = outputFeatureBuilder_cardinalpoint.buildFeature(null);
			
			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
			Coordinate[] CardinalPointsCoords = new Coordinate[caCardinalPoints.length()];
			   
			for (int i = 0; i < caCardinalPoints.length(); i++) {
				JSONArray xy = caCardinalPoints.getJSONArray(i);
				CardinalPointsCoords[i] = new Coordinate(xy.getDouble(0), xy.getDouble(1));
			}

			MultiPoint geomCardinalPoints = geometryFactory.createMultiPoint(CardinalPointsCoords);
			
			outputf_cardinalpoint.setAttribute("the_geom", geomCardinalPoints);
			outputf_cardinalpoint.setAttribute("caCode", caInfo.getString("caCode"));
			outputf_cardinalpoint.setAttribute("caName", caInfo.getString("caName"));
			outputf_cardinalpoint.setAttribute("jobId", simId);
			outputf_cardinalpoint.setAttribute("ptNum", caCardinalPoints.length());
			
			outputFC_cardinalpoint.add(outputf_cardinalpoint);

		}
		
		fIteratorOriginal.close();
		fIterator.close();
		
		wfsDS.dispose();
		
		///////////////////// export shpfile
		//sort iop: lower iop value has higher rank
		for(int k = 0; k<layerList.length(); k++){
			
			String strAbbr = layerList.getJSONObject(k).getString("layernameabbr");
			final String strColName = "iop_"+strAbbr;
			Collections.sort(outputFL, new Comparator<SimpleFeature>() {
			    @Override
			    public int compare(SimpleFeature a, SimpleFeature b) {
			            if((double)a.getAttribute(strColName) > (double)b.getAttribute(strColName)){
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
	
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType, outputFC, new File(outputShpFilePath_iop), true, CRS.decode("EPSG:4326"));
		
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType_cardinalpoint, outputFC_cardinalpoint, new File(outputShpFilePath_iop_cardinalpoint), true, CRS.decode("EPSG:28355"));

		///////////////////// prepare data for geoserver publishing
		JSONArray vectors = new JSONArray();
		// add ca polygon layer
		JSONObject vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:4326");
		vec.put("path", outputShpFilePath_iop);
		vec.put("name", simCode+"_iop");
		vec.put("layertype", "polygon");
		vec.put("layerclass", "iop");
		vectors.put(vec);
		
		// add ca control point layer
	    vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:28355");
		vec.put("path", outputShpFilePath_iop_cardinalpoint);
		vec.put("name", simCode+"_iop_cardinalpoint");
		vec.put("layertype", "point");
		vec.put("layerclass", "iop_cardinalpoint");
		
		vectors.put(vec);
		
		output.put("infos", infoArray);
		output.put("vectors", vectors);
		
		return output;
	}
	
	public static LineStringGraphGenerator calcRoadNetwork(JSONArray allodpoints, BoundingBox caBbox) throws DataSourceException, IOException, NoSuchAuthorityCodeException, FactoryException, SchemaException, JSONException{
		
		WFSDataStore wfsDS = new WFSDataStore();
		SimpleFeatureType schema = wfsDS.getDataStore().getSchema(AppConfig.getString("constantLAYERNAME_ROADNETWORK"));
		SimpleFeatureSource source = wfsDS.getFeatureSource(AppConfig.getString("constantLAYERNAME_ROADNETWORK"));
		
		Name geomName = schema.getGeometryDescriptor().getName();
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		
		CoordinateReferenceSystem targetCRS = schema.getGeometryDescriptor().getCoordinateReferenceSystem();
	    
		double[] bboxArr = getScaledBboxCoords(caBbox);
		
	    ReferencedEnvelope bboxEnv = new ReferencedEnvelope(bboxArr[0], 
												    		bboxArr[1], 
												    		bboxArr[2], 
												    		bboxArr[3], targetCRS);
	    
	    // apply AND filter: bbox and class_code
	    Filter filter = ff.and(
	    							ff.bbox(ff.property(geomName), bboxEnv), 
	    							ff.less(ff.property("class_code"), ff.literal(AppConfig.getString("constantROADNETWORK_CLASS_CODE")))
	    						);
	  
	    SimpleFeatureCollection fc = source.getFeatures(filter);
	    
	    
	    Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
		CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
		CoordinateReferenceSystem CRS4326 = factory.createCoordinateReferenceSystem("EPSG:4326");
		CoordinateReferenceSystem CRS28355 = factory.createCoordinateReferenceSystem("EPSG:28355");
		
	    
	    //very important to force fc into CRS4326 before doing the reprojecting to CRS28355. otherwise, odd coords will be generated.
	    //ref: http://www.massapi.com/source/geoserver-2.1.1-src/geoserver-2.1.1/extension/wps/wps-core/src/main/java/org/geoserver/wps/gs/ReprojectProcess.java.html
	    SimpleFeatureCollection fc28355 = new ForceCoordinateSystemFeatureResults(fc, CRS4326, false);
	    fc28355 = new ReprojectingFeatureCollection(fc28355, CRS28355);
		
	    
	    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(0));
	    
	    // put all origins and destinations into one list for merging
		List<Point> tomergepoints = new ArrayList<Point>();
		for(int i = 0; i<allodpoints.length(); i++){
			Point p = geometryFactory.createPoint(new Coordinate(allodpoints.getJSONArray(i).getDouble(0), allodpoints.getJSONArray(i).getDouble(1)));
    		tomergepoints.add(p);
		}
		
		return PathGenerator.getLineStringGraphGenerator(fc28355, tomergepoints);
	}
	
	public static double calcMeanDistance(JSONArray origins, JSONArray destinations, LineStringGraphGenerator lineStringGraphGenerator) throws NoSuchAuthorityCodeException, IOException, FactoryException, SchemaException, JSONException{
		
		double result = 0.0;
	    
	    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(0));
		
		// prepare odpair list 
		List<ODPair> odpairList = new ArrayList<ODPair>();
		for(int i = 0; i<origins.length(); i++){
			
			Point oPoint = geometryFactory.createPoint(new Coordinate(origins.getJSONArray(i).getDouble(0), origins.getJSONArray(i).getDouble(1)));
			
			for(int j = 0; j<destinations.length(); j++){
				
				Point dPoint = geometryFactory.createPoint(new Coordinate(destinations.getJSONArray(j).getDouble(0), destinations.getJSONArray(j).getDouble(1)));

				ODPair od = new ODPair();
				od.oPoint = oPoint;
				od.dPoint = dPoint;
				odpairList.add(od);
			}
		}
		
		// calc the mean distance
		double ttlDistance = 0.0;
		List<Path> pathList = PathGenerator.shortestPaths(lineStringGraphGenerator, odpairList);
		System.out.println("======  found paths :"+pathList.size());
		for(int i=0; i<pathList.size(); i++){
			
			
			Path odPath = pathList.get(i);
			List<Edge> edges = (List<Edge>)odPath.getEdges();
			
			for(int j = 0;j < edges.size(); j++){

				LineString line = (LineString)(edges.get(j)).getObject();
				ttlDistance = ttlDistance + line.getLength();
				
			}
			
		}
		
		if(pathList.size()>0){
			result = ttlDistance/pathList.size();
		}
		
		return result;
		
	}
	
	public static double[] getScaledBboxCoords(BoundingBox caBbox){
		
		double[] coords = new double[]{0.0,0.0,0.0,0.0};
		
		double maxX = caBbox.getMaxX();
		double maxY = caBbox.getMaxY();
		double minX = caBbox.getMinX();
		double minY = caBbox.getMinY();
		
		double xDistance = (maxX-minX)/2.0;
		double yDistance = (maxY-minY)/2.0;
		
		double scaleFactor = Double.parseDouble(AppConfig.getString("constantBBOX_SCALE_FACTOR"));
		coords[0] = minX - xDistance * scaleFactor;
		coords[1] = maxX + xDistance * scaleFactor;
		coords[2] = minY - yDistance * scaleFactor;
		coords[3] = maxY + yDistance * scaleFactor;

		return coords;
		
	}

}
