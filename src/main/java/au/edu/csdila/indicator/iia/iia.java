package au.edu.csdila.indicator.iia;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.io.FileUtils;
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
import org.opengis.referencing.FactoryException;
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
public class iia {
	
	static final Logger LOGGER = LoggerFactory.getLogger(iia.class);
	
	/**
	 * 
	 * @param options
	 * @return
	 * @throws Exception
	 */
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
		String MBLayerName = AppConfig.getString("constantLAYERNAME_MESHBLOCK");
		String FOILayerName = AppConfig.getString("constantLAYERNAME_FOI");
		String FOIPolygonLayerName = AppConfig.getString("constantLAYERNAME_FOI_POLYGON");
		String NetworkLayerName = AppConfig.getString("constantLAYERNAME_ROADNETWORK");
		
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
		
		SimpleFeatureType schemaFOIPolygon = wfsDS.getDataStore().getSchema(FOIPolygonLayerName);
		SimpleFeatureSource sourceFOIPolygon = wfsDS.getFeatureSource(FOIPolygonLayerName);
		Name geomNameFOIPolygon = schemaFOIPolygon.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaNetwork = wfsDS.getDataStore().getSchema(NetworkLayerName);
		SimpleFeatureSource sourceNetwork = wfsDS.getFeatureSource(NetworkLayerName);
		Name geomNameNetwork = schemaNetwork.getGeometryDescriptor().getName();
		
		
		SimpleFeatureType schemaTrain = wfsDS.getDataStore().getSchema(TrainStationLayerName);
		SimpleFeatureSource sourceTrain = wfsDS.getFeatureSource(TrainStationLayerName);
		Name geomNameTrain = schemaTrain.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaTram = wfsDS.getDataStore().getSchema(TramStopLayerName);
		SimpleFeatureSource sourceTram = wfsDS.getFeatureSource(TramStopLayerName);
		Name geomNameTram = schemaTram.getGeometryDescriptor().getName();
		
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
			    
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
		JSONArray layerList = options.getJSONArray("layerList");
		String tmpOutputFolderPath = options.getString("tmpOutputFolderPath");
		boolean enableUPQ = options.getBoolean("enableUPQ");
		boolean enableRankAmongAllLGAs = options.getBoolean("enableRankAmongAllLGAs");
		double iiaBufferDist = options.getDouble("iiaBufferDist");
		double iiaMinMeshBlockPopThreshold = options.getDouble("iiaMinMeshBlockPopThreshold");
		
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
		 * /tmpout/iia/username/jobcode_iia/jobcode_iia.shp
		*/
		
		//iia.shp is based on MeshBlock geom. 
		File f_iia = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_iia"));
		if(f_iia.exists()){FileUtils.deleteDirectory(f_iia);}	
		f_iia.mkdirs();
		String outputShpFilePath_iia = f_iia.getAbsolutePath()+File.separator+simCode+"_iia.shp";
		String outputShpFilePath_tmp = f_iia.getAbsolutePath()+File.separator+simCode+"_tmp_####_@@@@.shp";
		
		//netbuf.shp is created for each type of services, an servicetype column will be created to indicate which service type this buffer geom belongs to
		File f_netbuf = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_netbuf"));
		if(f_netbuf.exists()){FileUtils.deleteDirectory(f_netbuf);}	
		f_netbuf.mkdirs();
		String outputShpFilePath_netbuf = f_netbuf.getAbsolutePath()+File.separator+simCode+"_netbuf.shp";

		//filteredMB.shp is created for each type of services, an servicetype column will be created to indicate which service type this buffer geom belongs to
		File f_filteredMB = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_filteredMB"));
		if(f_filteredMB.exists()){FileUtils.deleteDirectory(f_filteredMB);}	
		f_filteredMB.mkdirs();
		String outputShpFilePath_filteredMB = f_filteredMB.getAbsolutePath()+File.separator+simCode+"_filteredMB.shp";
				
		
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
	    
	    //////// prepare iia output shp file structure
		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
		stb.setName("iia");
		// define 'the_geom' as MultiPolygon for shp file
		stb.add("the_geom", MultiPolygon.class);
		stb.setDefaultGeometry("the_geom");
		for (AttributeDescriptor attDisc : schemaMB.getAttributeDescriptors()) {
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
			 
			 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
			 //retain iia and weight for each type of services
			 stb.add("iia_"+strAbbr, Double.class);
			 stb.add("wgt_"+strAbbr, Double.class);

		 }
				 
		 //add two additional attribute to record iia and upq for each MB
		//add two additional attribute to record iia and upq rank for each MB
		 stb.add("iia", Double.class);
		 stb.add("iia_r", Double.class);
		 if(enableUPQ){
			 stb.add("upq", Double.class);
			 stb.add("upq_r", Double.class);
		 }
		 
		 //calc and rank iia upq among all LGAs
		 if(enableRankAmongAllLGAs){
			 stb.add("iiag_r", Double.class);
			 if(enableUPQ){
				 stb.add("upqg_r", Double.class);
			 }
		 }
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType = stb.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC =  new DefaultFeatureCollection();
		 // put feature into list first, then we can sort and create the rank, then we put it into featurecollection for output 
		 List<SimpleFeature> outputFL = new ArrayList<SimpleFeature>();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder = new SimpleFeatureBuilder(outputFeatureType);
		 
		 
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
		 
		 DefaultFeatureCollection fcMBFiltered =  new DefaultFeatureCollection();
		 
		 //main loop (ca LGA level) starts here
		fIterator = fcLGA.features();
		while(fIterator.hasNext()){
			
		    //prepare caInfo;
		    JSONObject caInfo = new JSONObject();
			
			SimpleFeatureImpl feature =  (SimpleFeatureImpl)fIterator.next();
			//this the original polygon of lga
			Geometry polygon = (Geometry)feature.getDefaultGeometry();
			//the default geom is in 4326, need to convert to 28355 to create the buffer then convert it back. Annoying!
			Geometry polygon28355 = JTS.transform(polygon, transform28355);
			Geometry polygonBuffer28355 = polygon28355.buffer(iiaBufferDist);
			Geometry polygonBuffer = JTS.transform(polygonBuffer28355, transform4326);
			
		    caInfo.put("caCode", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_CODE")));
		    caInfo.put("caName", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_NAME")).toString().trim());
		    

			 //////////////////// step 0. calc MB within ca

			 Filter filterMB = ff.intersects(ff.property(geomNameMB), ff.literal(polygon));
			 
			 SimpleFeatureCollection rawfcMB = sourceMB.getFeatures(filterMB);
			 DefaultFeatureCollection fcMBUntrimmed =  new DefaultFeatureCollection();
			 
			 SimpleFeatureIterator fIteratorMB = rawfcMB.features();

			 while (fIteratorMB.hasNext()) 
		     {
				 fcMBUntrimmed.add(fIteratorMB.next());
			 }
			 fIteratorMB.close();
			 
			 SimpleFeatureCollection fcMB = GeometryUtils.intersection(fcMBUntrimmed, polygon, 0.8);
			 
			 //use a temp fc to hold current LGA's MB with initial iia and upq values
			 DefaultFeatureCollection fcMBTmp =  new DefaultFeatureCollection();
			 
			 fIteratorMB = fcMB.features();
			 while (fIteratorMB.hasNext()) 
		     {				
				 SimpleFeature outputf = outputFeatureBuilder.buildFeature(null);
				 
				 SimpleFeatureImpl f =  (SimpleFeatureImpl)fIteratorMB.next();
				// init output feature attributes with sa1 attributes, those appending iia/upq attributes will be calculated later
				 for (AttributeDescriptor attDisc : schemaMB.getAttributeDescriptors()) {
						String name = attDisc.getLocalName();
						// if current field is a geometry, we save it to the appointed field 'the_geom'
						if (attDisc instanceof GeometryDescriptor) {
							outputf.setAttribute("the_geom", f.getAttribute(name));
						} else {// otherwise, copy field value respectively
						
							outputf.setAttribute(ShapeFileUtils.clapFieldName(name), f.getAttribute(name));
							//LOGGER.info("attrname:{} value:{}", ShapeFileUtils.clapFieldAgePopName(name), outputf_isa.getAttribute(ShapeFileUtils.clapFieldAgePopName(name)));
						}
				 }
				 outputf.setAttribute("iia", 0.0);
				 if(enableUPQ){
					 outputf.setAttribute("upq", 0.0);
				 }
				 
				 
				 for(int i=0; i<layerList.length(); i++){
					 
					 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
					 outputf.setAttribute("iia_"+strAbbr, 0.0);
					 outputf.setAttribute("wgt_"+strAbbr, 0.0);

					 //retain iia and weight for each type of services
				 }
				
				// add a mb population threshold filter
				 if(iiaMinMeshBlockPopThreshold>0){
					 if(Double.parseDouble(f.getAttribute(AppConfig.getString("constantATTRNAME_MESHBLOCK_POPULATION_NAME")).toString()) >= iiaMinMeshBlockPopThreshold){
						 fcMBTmp.add(outputf);
					 }
					 else{
						 fcMBFiltered.add(outputf);
					 }
				 }else{
					 fcMBTmp.add(outputf);
				 }
				 
			 }
			 fIteratorMB.close();
			 
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
				
				
				DefaultFeatureCollection fcFOI =  new DefaultFeatureCollection();
				SimpleFeatureIterator fIteratorFOI = rawfcFOI.features();

				while (fIteratorFOI.hasNext()) 
			    {
					 fcFOI.add(fIteratorFOI.next());
				}
				fIteratorFOI.close();
				 
				//if this type of service does exist in currrent ca
				if(fcFOI.size() > 0)
				{

					// catchment radius MUST be assgined, treat as polygon intersection 
						
					// substep 1. calc network buffer 
					SimpleFeatureCollection fcNB = null;
					NumberFormat formatter = new DecimalFormat("#0.00000");
					
					//Gus's network buffer method
					long start = System.currentTimeMillis();
					au.edu.csdila.common.connectivity.NetworkBufferBatch nbb = new au.edu.csdila.common.connectivity.NetworkBufferBatch(sourceNetwork, fcFOI, layerList.getJSONObject(i).getDouble("catchmentradius"), netbufSize);
					SimpleFeatureCollection fcNB28355 = nbb.createBuffers();
					fcNB = new ForceCoordinateSystemFeatureResults(fcNB28355, CRS28355, false);
					fcNB = new ReprojectingFeatureCollection(fcNB, CRS4326);
					long end = System.currentTimeMillis();
					LOGGER.info("==== method 1 Execution time is:{} seconds", formatter.format((end - start) / 1000d));
					
					
					// we need create a brand new featurecollection to hold the result
					DefaultFeatureCollection fcnetbuf =  new DefaultFeatureCollection();
					
					SimpleFeatureIterator iteratorNB= fcNB.features();
					while (iteratorNB.hasNext()) 
			        {
						SimpleFeature outputf_netbuf = outputFeatureBuilder_netbuf.buildFeature(null);
						
						SimpleFeatureImpl fNB =  (SimpleFeatureImpl)iteratorNB.next();
			            
			            outputf_netbuf.setAttribute("the_geom", fNB.getDefaultGeometry());
			            outputf_netbuf.setAttribute("servicetyp", strAbbr);
			            fcnetbuf.add(outputf_netbuf);
			            
			            outputFC_netbuf.add(outputf_netbuf);

			        }
					iteratorNB.close();
					
					// substep 2. union network buffer's geometries
					String tmpoutfilepath = outputShpFilePath_tmp.replaceAll("####", strAbbr);
				    tmpoutfilepath = tmpoutfilepath.replaceAll("@@@@", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_CODE")).toString().trim());
					SimpleFeatureCollection fcUnion = cleanSimpleFeatureCollection(fcnetbuf, new File(tmpoutfilepath),true, CRS.decode("EPSG:4326"));
					Geometry unionedNB = GeometryUtils.union(fcUnion);

					// substep 3. calc (a) quantity of MB, (b)iia of MB and (c) UPQ of MB by computing the overlapped area of the geom of MB and unioned network buffer
					fIteratorMB = fcMBTmp.features();
					 while (fIteratorMB.hasNext()) 
				     {				
						 SimpleFeature f =  fIteratorMB.next();
						 Geometry geomMB = (Geometry)f.getDefaultGeometry();
						 double areaMB = geomMB.getArea();
						 double overlapQuantity = unionedNB.intersection(geomMB).getArea();
						 double iiaTotal = Double.parseDouble(f.getAttribute("iia").toString());
						 double iiaCurrent = 0.0;
						 double wgtCurrent = Double.parseDouble(layerList.getJSONObject(i).getString("weight"));
						 double upq = 0.0;
						 if(areaMB>0.0){
							 iiaCurrent = overlapQuantity * wgtCurrent / (100 * areaMB);
							 iiaTotal = iiaCurrent + iiaTotal;
							 double pop = Double.parseDouble(f.getAttribute("population").toString());
							 if(pop>0){
								 upq = iiaTotal/pop;
							 }
						 }
						 
						 f.setAttribute("iia", iiaTotal);
						 f.setAttribute("iia_"+strAbbr, iiaCurrent);

						 f.setAttribute("wgt_"+strAbbr, wgtCurrent);
						 if(enableUPQ){
							 f.setAttribute("upq", upq);
						 }
					 }
					 fIteratorMB.close();
					
					
				}
				
			}
			
			
			// create and iia and upq rank within current lga
			List<SimpleFeature> tmpLGAOutputFL = new ArrayList<SimpleFeature>();
			DefaultFeatureCollection tmpLGAOutputFC =  new DefaultFeatureCollection();
			//store iia and upq filled MBs into final output feature collection
			
			fIteratorMB = fcMBTmp.features();
			 while (fIteratorMB.hasNext()) 
		     {
				SimpleFeature ftmp = fIteratorMB.next();
				tmpLGAOutputFL.add(ftmp);
			 }
			 fIteratorMB.close();
			 
			 Collections.sort(tmpLGAOutputFL, new Comparator<SimpleFeature>() {
				    @Override
				    public int compare(SimpleFeature a, SimpleFeature b) {
				            if((double)a.getAttribute("iia") < (double)b.getAttribute("iia")){
				            	return 1;
				            }else if((double)a.getAttribute("iia") == (double)b.getAttribute("iia")){
				            	return 0;
				            }
				            else{
				            	return -1;
				            }
				    }
				});

				int rank_iia = 1;
				int jump_iia = 0;
				for(int i=0; i<tmpLGAOutputFL.size();i++){
					tmpLGAOutputFL.get(i).setAttribute("iia_r", rank_iia);

					//if not the last element
					if(i < tmpLGAOutputFL.size() -1)
					{
						//only increase the rank if current value is NOT equal to the next value
						if(!MathUtils.doubleEquals((double)tmpLGAOutputFL.get(i).getAttribute("iia"), (double)tmpLGAOutputFL.get(i+1).getAttribute("iia"), 0.000000001))
						{
							rank_iia = rank_iia + jump_iia + 1;
							jump_iia = 0;
						}else
						{
							jump_iia++;
						}
					}
				}

				if(enableUPQ){
					//sort upq
					Collections.sort(tmpLGAOutputFL, new Comparator<SimpleFeature>() {
					    @Override
					    public int compare(SimpleFeature a, SimpleFeature b) {
					            if((double)a.getAttribute("upq") < (double)b.getAttribute("upq")){
					            	return 1;
					            }else if((double)a.getAttribute("upq") == (double)b.getAttribute("upq")){
					            	return 0;
					            }
					            else{
					            	return -1;
					            }
					    }
					});
			
					int rank_upq = 1;
					int jump_upq = 0;
					for(int i=0; i<tmpLGAOutputFL.size();i++){
						tmpLGAOutputFL.get(i).setAttribute("upq_r", rank_upq);
			
						//if not the last element
						if(i < tmpLGAOutputFL.size() -1)
						{
							//only increase the rank if current value is NOT equal to the next value
							if(!MathUtils.doubleEquals((double)tmpLGAOutputFL.get(i).getAttribute("upq"), (double)tmpLGAOutputFL.get(i+1).getAttribute("upq"), 0.000000001))
							{
								rank_upq = rank_upq + jump_upq + 1;
								jump_upq = 0;
							}else
							{
								jump_upq++;
							}
						}
					}
				}
				
				//all sorting and ranking done, put it featureCollection
				for(int i=0; i<tmpLGAOutputFL.size();i++){
					outputFL.add(tmpLGAOutputFL.get(i));
				}
		}
		
		//handle ranks among LGAs
		if(enableRankAmongAllLGAs){
			
			//then create the rank for iiag and upqg
			Collections.sort(outputFL, new Comparator<SimpleFeature>() {
			    @Override
			    public int compare(SimpleFeature a, SimpleFeature b) {
			            if((double)a.getAttribute("iia") < (double)b.getAttribute("iia")){
			            	return 1;
			            }else if((double)a.getAttribute("iia") == (double)b.getAttribute("iia")){
			            	return 0;
			            }
			            else{
			            	return -1;
			            }
			    }
			});

			int rank_iiag = 1;
			int jump_iiag = 0;
			for(int i=0; i<outputFL.size();i++){
				outputFL.get(i).setAttribute("iiag_r", rank_iiag);

				//if not the last element
				if(i < outputFL.size() -1)
				{
					//only increase the rank if current value is NOT equal to the next value
					if(!MathUtils.doubleEquals((double)outputFL.get(i).getAttribute("iia"), (double)outputFL.get(i+1).getAttribute("iia"), 0.000000001))
					{
						rank_iiag = rank_iiag + jump_iiag + 1;
						jump_iiag = 0;
					}else
					{
						jump_iiag++;
					}
				}
			}

			if(enableUPQ){
				//sort upq
				Collections.sort(outputFL, new Comparator<SimpleFeature>() {
				    @Override
				    public int compare(SimpleFeature a, SimpleFeature b) {
				            if((double)a.getAttribute("upq") < (double)b.getAttribute("upq")){
				            	return 1;
				            }else if((double)a.getAttribute("upq") == (double)b.getAttribute("upq")){
				            	return 0;
				            }
				            else{
				            	return -1;
				            }
				    }
				});
		
				int rank_upqg = 1;
				int jump_upqg = 0;
				for(int i=0; i<outputFL.size();i++){
					outputFL.get(i).setAttribute("upqg_r", rank_upqg);
		
					//if not the last element
					if(i < outputFL.size() -1)
					{
						//only increase the rank if current value is NOT equal to the next value
						if(!MathUtils.doubleEquals((double)outputFL.get(i).getAttribute("upq"), (double)outputFL.get(i+1).getAttribute("upq"), 0.000000001))
						{
							rank_upqg = rank_upqg + jump_upqg + 1;
							jump_upqg = 0;
						}else
						{
							jump_upqg++;
						}
					}
				}
			}
		}
		
		//all sorting and ranking done, put it featureCollection
		for(int i=0; i<outputFL.size();i++){
			outputFC.add(outputFL.get(i));
		}
		
		fIterator.close();
		wfsDS.dispose();

		///////////////////// export shpfile
		
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType, outputFC, new File(outputShpFilePath_iia), true, CRS.decode("EPSG:4326"));
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType_netbuf, outputFC_netbuf, new File(outputShpFilePath_netbuf), true, CRS.decode("EPSG:4326"));

		if(iiaMinMeshBlockPopThreshold>0 && fcMBFiltered.size()>0){
			ShapeFileUtils.featuresExportToShapeFile(outputFeatureType, fcMBFiltered, new File(outputShpFilePath_filteredMB), true, CRS.decode("EPSG:4326"));
		}
			
		///////////////////// prepare data for geoserver publishing
		JSONArray vectors = new JSONArray();
		// add iia layer
		JSONObject vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:4326");
		vec.put("path", outputShpFilePath_iia);
		vec.put("name", simCode+"_iia");
		vec.put("layertype", "polygon");
		vec.put("layerclass", "iia");
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
		
		// add filteredMB layer if necessary
		if(iiaMinMeshBlockPopThreshold>0 && fcMBFiltered.size()>0){
			vec = new JSONObject();
			vec.put("forcesrs", "EPSG:4326");
			vec.put("nativesrs", "EPSG:4326");
			vec.put("path", outputShpFilePath_filteredMB);
			vec.put("name", simCode+"_filteredMB");
			vec.put("layertype", "polygon");
			vec.put("layerclass", "filteredMB");
			vectors.put(vec);
		}
		output.put("infos", infoArray);
		output.put("vectors", vectors);
		
		return output;
	}

	
	/**
	 * this is old method, which computes IIA without taking into accout of the area of MB, and it is problematic since IIA becomes correlated to the size of MB
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static JSONObject exec_old(JSONObject options) throws Exception{
		
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
		String MBLayerName = AppConfig.getString("constantLAYERNAME_MESHBLOCK");
		String FOILayerName = AppConfig.getString("constantLAYERNAME_FOI");
		String FOIPolygonLayerName = AppConfig.getString("constantLAYERNAME_FOI_POLYGON");
		String NetworkLayerName = AppConfig.getString("constantLAYERNAME_ROADNETWORK");
		
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
		
		SimpleFeatureType schemaFOIPolygon = wfsDS.getDataStore().getSchema(FOIPolygonLayerName);
		SimpleFeatureSource sourceFOIPolygon = wfsDS.getFeatureSource(FOIPolygonLayerName);
		Name geomNameFOIPolygon = schemaFOIPolygon.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaNetwork = wfsDS.getDataStore().getSchema(NetworkLayerName);
		SimpleFeatureSource sourceNetwork = wfsDS.getFeatureSource(NetworkLayerName);
		Name geomNameNetwork = schemaNetwork.getGeometryDescriptor().getName();
		
		
		SimpleFeatureType schemaTrain = wfsDS.getDataStore().getSchema(TrainStationLayerName);
		SimpleFeatureSource sourceTrain = wfsDS.getFeatureSource(TrainStationLayerName);
		Name geomNameTrain = schemaTrain.getGeometryDescriptor().getName();
		
		SimpleFeatureType schemaTram = wfsDS.getDataStore().getSchema(TramStopLayerName);
		SimpleFeatureSource sourceTram = wfsDS.getFeatureSource(TramStopLayerName);
		Name geomNameTram = schemaTram.getGeometryDescriptor().getName();
		
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
			    
	    //projection 
	    Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
		CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
		CoordinateReferenceSystem CRS4326 = factory.createCoordinateReferenceSystem("EPSG:4326");
		CoordinateReferenceSystem CRS28355 = factory.createCoordinateReferenceSystem("EPSG:28355");
		
		
		// get input params
		
		JSONArray regionCodeList = options.getJSONArray("regionCodeList");
		JSONArray layerList = options.getJSONArray("layerList");
		String tmpOutputFolderPath = options.getString("tmpOutputFolderPath");
		boolean enableUPQ = options.getBoolean("enableUPQ");
		boolean enableRankAmongAllLGAs = options.getBoolean("enableRankAmongAllLGAs");
		
		double netbufSize = options.getDouble("netbufSize");
		double maxCatchmentRadius = 0.0;
		for(int i=0; i<layerList.length(); i++){
			if(layerList.getJSONObject(i).getDouble("catchmentradius") > maxCatchmentRadius){	
				maxCatchmentRadius = layerList.getJSONObject(i).getDouble("catchmentradius");
			}
		}
		
		if(enableRankAmongAllLGAs){
			for(int i=0; i<layerList.length(); i++){
				layerList.getJSONObject(i).put("sumMaxQuantity", 0.0f);
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
		 * /tmpout/iia/username/jobcode_iia/jobcode_iia.shp
		*/
		
		//iia.shp is based on MeshBlock geom. 
		File f_iia = new File(String.format("%s%s%s", tmpOutputFolderPath , File.separator, simCode+"_iia"));
		if(f_iia.exists()){FileUtils.deleteDirectory(f_iia);}	
		f_iia.mkdirs();
		String outputShpFilePath_iia = f_iia.getAbsolutePath()+File.separator+simCode+"_iia.shp";
		String outputShpFilePath_tmp = f_iia.getAbsolutePath()+File.separator+simCode+"_tmp_####_@@@@.shp";
		
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
	    
	    //////// prepare iia output shp file structure
		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
		stb.setName("iia");
		// define 'the_geom' as MultiPolygon for shp file
		stb.add("the_geom", MultiPolygon.class);
		stb.setDefaultGeometry("the_geom");
		for (AttributeDescriptor attDisc : schemaMB.getAttributeDescriptors()) {
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
			 
			 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
			 //retain iia and weight for each type of services
			 stb.add("iia_"+strAbbr, Double.class);
			 stb.add("wgt_"+strAbbr, Double.class);
			 
			 if(enableRankAmongAllLGAs){
				 stb.add("iiag_"+strAbbr, Double.class);
			 }
		 }
				 
		 //add two additional attribute to record iia and upq for each MB
		//add two additional attribute to record iia and upq rank for each MB
		 stb.add("iia", Double.class);
		 stb.add("iia_r", Double.class);
		 if(enableUPQ){
			 stb.add("upq", Double.class);
			 stb.add("upq_r", Double.class);
		 }
		 
		 //calc and rank iia upq among all LGAs
		 if(enableRankAmongAllLGAs){
			 stb.add("iiag", Double.class);
			 stb.add("iiag_r", Double.class);
			 if(enableUPQ){
				 stb.add("upqg", Double.class);
				 stb.add("upqg_r", Double.class);
			 }
		 }
		 // now we have a new SimpleFeatureType which suits for shp file exporting as well
		 SimpleFeatureType outputFeatureType = stb.buildFeatureType();
		 // we need create a brand new featurecollection to hold the result
		 DefaultFeatureCollection outputFC =  new DefaultFeatureCollection();
		 // put feature into list first, then we can sort and create the rank, then we put it into featurecollection for output 
		 List<SimpleFeature> outputFL = new ArrayList<SimpleFeature>();
		 // create a FeatureBuilder to build features
		 SimpleFeatureBuilder outputFeatureBuilder = new SimpleFeatureBuilder(outputFeatureType);
		 
		 
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
			//boolean lenient = true; // allow for some error due to different datums
		    //MathTransform transform28355 = CRS.findMathTransform(CRS4326, CRS28355, lenient);
		    //MathTransform transform4326 = CRS.findMathTransform(CRS28355, CRS4326, lenient);
		    //pointOfInterest is in 28355
		    //Geometry polygon28355 = JTS.transform(polygon, transform28355);
			//Geometry polygonBuffer28355 = polygon28355.buffer(maxCatchmentRadius + netbufSize);
			//then change buffer back to 4326
			//Geometry polygonBuffer = JTS.transform(polygonBuffer28355, transform4326);
			
		    caInfo.put("caCode", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_CODE")));
		    caInfo.put("caName", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_NAME")).toString().trim());
		    

			 //////////////////// step 0. calc MB within ca
			 
			 Filter filterMB = ff.intersects(ff.property(geomNameMB), ff.literal(polygon));
			 SimpleFeatureCollection rawfcMB = sourceMB.getFeatures(filterMB);
			 DefaultFeatureCollection fcMBUntrimmed =  new DefaultFeatureCollection();
			 
			 SimpleFeatureIterator fIteratorMB = rawfcMB.features();

			 while (fIteratorMB.hasNext()) 
		     {
				 fcMBUntrimmed.add(fIteratorMB.next());
			 }
			 fIteratorMB.close();
			 
			 SimpleFeatureCollection fcMB = GeometryUtils.intersection(fcMBUntrimmed, polygon, 0.8);
			 
			 //use a temp fc to hold current LGA's MB with initial iia and upq values
			 DefaultFeatureCollection fcMBTmp =  new DefaultFeatureCollection();
			 
			 fIteratorMB = fcMB.features();
			 while (fIteratorMB.hasNext()) 
		     {				
				 SimpleFeature outputf = outputFeatureBuilder.buildFeature(null);
				 
				 SimpleFeatureImpl f =  (SimpleFeatureImpl)fIteratorMB.next();
				// init output feature attributes with sa1 attributes, those appending iia/upq attributes will be calculated later
				 for (AttributeDescriptor attDisc : schemaMB.getAttributeDescriptors()) {
						String name = attDisc.getLocalName();
						// if current field is a geometry, we save it to the appointed field 'the_geom'
						if (attDisc instanceof GeometryDescriptor) {
							outputf.setAttribute("the_geom", f.getAttribute(name));
						} else {// otherwise, copy field value respectively
						
							outputf.setAttribute(ShapeFileUtils.clapFieldName(name), f.getAttribute(name));
							//LOGGER.info("attrname:{} value:{}", ShapeFileUtils.clapFieldAgePopName(name), outputf_isa.getAttribute(ShapeFileUtils.clapFieldAgePopName(name)));
						}
				 }
				 outputf.setAttribute("iia", 0.0);
				 if(enableUPQ){
					 outputf.setAttribute("upq", 0.0);
				 }
				 
				 if(enableRankAmongAllLGAs){
					 outputf.setAttribute("iiag", 0.0);
					 if(enableUPQ){
						 outputf.setAttribute("upqg", 0.0);
					 }
				 }
				 
				 for(int i=0; i<layerList.length(); i++){
					 
					 String strAbbr = layerList.getJSONObject(i).getString("layernameabbr");
					 outputf.setAttribute("iia_"+strAbbr, 0.0);
					 outputf.setAttribute("wgt_"+strAbbr, 0.0);

					 if(enableRankAmongAllLGAs){
						 outputf.setAttribute("iiag_"+strAbbr, 0.0);
					 }
					 //retain iia and weight for each type of services
				 }
				
				 fcMBTmp.add(outputf);
				 
			 }
			 fIteratorMB.close();
			 
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
				//handle differently for 'trainstation' and 'tramstop'
				if(layerList.getJSONObject(i).getString("layername").equalsIgnoreCase("trainstation")){
					
					rawfcFOI = sourceTrain.getFeatures(ff.intersects(ff.property(geomNameTrain), ff.literal(polygon)));
					
				}else if(layerList.getJSONObject(i).getString("layername").equalsIgnoreCase("tramstop")){
					
					rawfcFOI = sourceTram.getFeatures(ff.intersects(ff.property(geomNameTram), ff.literal(polygon)));
				}
				else{
					
					filterFOI = ff.and(foiFilterOr, ff.intersects(ff.property(geomNameFOI), ff.literal(polygon)));
					rawfcFOI = sourceFOI.getFeatures(filterFOI);
				}
				
				
				DefaultFeatureCollection fcFOI =  new DefaultFeatureCollection();
				SimpleFeatureIterator fIteratorFOI = rawfcFOI.features();

				while (fIteratorFOI.hasNext()) 
			    {
					 fcFOI.add(fIteratorFOI.next());
				}
				fIteratorFOI.close();
				 
				//if this type of service does exist in currrent ca
				if(fcFOI.size() > 0)
				{
					/*
					SimpleFeatureCollection fcFOI28355 = new ForceCoordinateSystemFeatureResults(fcFOI, CRS4326, false);
					//the FOI CRS must be a projected one, e.g. EPSG:28355
					fcFOI28355 = new ReprojectingFeatureCollection(fcFOI28355, CRS28355);
					*/
					// calc target population of that service

					//if no catchment radius assgined, treat as point intersection 
					if(Double.parseDouble(layerList.getJSONObject(i).getString("catchmentradius"))<0.0001){

						// substep 1. calc fake network buffer by buffering the foi geom slightly (from point to polygon) so it can be added to the final netbuf shp file outputFC_netbuf
						fIteratorFOI = fcFOI.features();
						 while (fIteratorFOI.hasNext()) 
						 {
							 SimpleFeature outputf_netbuf = outputFeatureBuilder_netbuf.buildFeature(null);
								
							 SimpleFeatureImpl fFOI =  (SimpleFeatureImpl)fIteratorFOI.next();
							 Geometry geomFOI = (Geometry)fFOI.getDefaultGeometry();
							 Geometry geomFOIBuffer =  geomFOI.buffer(0.00001);
						            
					         outputf_netbuf.setAttribute("the_geom", geomFOIBuffer);
					         outputf_netbuf.setAttribute("servicetyp", strAbbr);
					         outputFC_netbuf.add(outputf_netbuf);
						 }
						 fIteratorFOI.close();
						 
						// substep 2. calc max quantity of LGA by counting the number of FOI it intersects. 
						double maxQuantity = fcFOI.size();
						
						if(enableRankAmongAllLGAs){
							double v = layerList.getJSONObject(i).getDouble("sumMaxQuantity") + maxQuantity;
							layerList.getJSONObject(i).put("sumMaxQuantity", v);
						}
						
						LOGGER.info("===== {} maxQuantity of LGA: {}", strAbbr, maxQuantity);
						// substep 3. calc (a) quantity of MB, (b)iia of MB and (c) UPQ of MB by counting the number of FOI it intersects. 
						fIteratorMB = fcMBTmp.features();
						 while (fIteratorMB.hasNext()) 
					     {				
							 SimpleFeature f =  fIteratorMB.next();
							 Geometry geomMB = (Geometry)f.getDefaultGeometry();
							 
							 double overlapQuantity = 0.0;
							 double iiaTotal = Double.parseDouble(f.getAttribute("iia").toString());
							 double iiaCurrent = 0.0;
							 double wgtCurrent = Double.parseDouble(layerList.getJSONObject(i).getString("weight"));
							 double upq = 0.0;
							 
							 fIteratorFOI = fcFOI.features();

							 while (fIteratorFOI.hasNext()) 
							 {
									 SimpleFeature fFOI =  fIteratorFOI.next();
									 Geometry geomFOI = (Geometry)fFOI.getDefaultGeometry();
									 if(geomMB.intersects(geomFOI)){
										 overlapQuantity = overlapQuantity+1.0;
									 }
							 }
							 fIteratorFOI.close();
							 
							 if(maxQuantity>0){
								 iiaCurrent = overlapQuantity * wgtCurrent / (100 * maxQuantity);
								 iiaTotal = iiaCurrent + iiaTotal;
								 double pop = Double.parseDouble(f.getAttribute("population").toString());
								 if(pop>0){
									 upq = iiaTotal/pop;
								 }
							 }
							 
							 f.setAttribute("iia", iiaTotal);
							 f.setAttribute("iia_"+strAbbr, iiaCurrent);
							 if(enableRankAmongAllLGAs){
								 f.setAttribute("iiag_"+strAbbr, overlapQuantity);
							 }
							 f.setAttribute("wgt_"+strAbbr, wgtCurrent);
							 
							 if(enableUPQ){
								 f.setAttribute("upq", upq);
							 }	
							 
						 }
						 fIteratorMB.close();

					}
					else{
						
						// substep 1. calc network buffer 
						SimpleFeatureCollection fcNB = null;
						NumberFormat formatter = new DecimalFormat("#0.00000");
						
						//Gus's network buffer method
						long start = System.currentTimeMillis();
						au.edu.csdila.common.connectivity.NetworkBufferBatch nbb = new au.edu.csdila.common.connectivity.NetworkBufferBatch(sourceNetwork, fcFOI, layerList.getJSONObject(i).getDouble("catchmentradius"), netbufSize);
						SimpleFeatureCollection fcNB28355 = nbb.createBuffers();
						fcNB = new ForceCoordinateSystemFeatureResults(fcNB28355, CRS28355, false);
						fcNB = new ReprojectingFeatureCollection(fcNB, CRS4326);
						long end = System.currentTimeMillis();
						LOGGER.info("==== method 1 Execution time is:{} seconds", formatter.format((end - start) / 1000d));
						
						
						// we need create a brand new featurecollection to hold the result
						DefaultFeatureCollection fcnetbuf =  new DefaultFeatureCollection();
						
						SimpleFeatureIterator iteratorNB= fcNB.features();
						while (iteratorNB.hasNext()) 
				        {
							SimpleFeature outputf_netbuf = outputFeatureBuilder_netbuf.buildFeature(null);
							
							SimpleFeatureImpl fNB =  (SimpleFeatureImpl)iteratorNB.next();
				            
				            outputf_netbuf.setAttribute("the_geom", fNB.getDefaultGeometry());
				            outputf_netbuf.setAttribute("servicetyp", strAbbr);
				            fcnetbuf.add(outputf_netbuf);
				            
				            outputFC_netbuf.add(outputf_netbuf);

				        }
						iteratorNB.close();
						
						// substep 2. union network buffer's geometries
						String tmpoutfilepath = outputShpFilePath_tmp.replaceAll("####", strAbbr);
					    tmpoutfilepath = tmpoutfilepath.replaceAll("@@@@", feature.getAttribute(AppConfig.getString("constantATTRNAME_LGA_CODE")).toString().trim());
						SimpleFeatureCollection fcUnion = cleanSimpleFeatureCollection(fcnetbuf, new File(tmpoutfilepath),true, CRS.decode("EPSG:4326"));
						Geometry unionedNB = GeometryUtils.union(fcUnion);

						// substep 3. calc max quantity of LGA by computing the overlapped area of the geom of LGA and unioned network buffer
						double maxQuantity = unionedNB.intersection(polygon).getArea();
						
						if(enableRankAmongAllLGAs){
							double v = layerList.getJSONObject(i).getDouble("sumMaxQuantity") + maxQuantity;
							layerList.getJSONObject(i).put("sumMaxQuantity", v);
						}
						
						LOGGER.info("===== {} maxQuantity of LGA: {}", strAbbr, maxQuantity);
						// substep 4. calc (a) quantity of MB, (b)iia of MB and (c) UPQ of MB by computing the overlapped area of the geom of MB and unioned network buffer
						fIteratorMB = fcMBTmp.features();
						 while (fIteratorMB.hasNext()) 
					     {				
							 SimpleFeature f =  fIteratorMB.next();
							 Geometry geomMB = (Geometry)f.getDefaultGeometry();
							 double overlapQuantity = unionedNB.intersection(geomMB).getArea();
							 double iiaTotal = Double.parseDouble(f.getAttribute("iia").toString());
							 double iiaCurrent = 0.0;
							 double wgtCurrent = Double.parseDouble(layerList.getJSONObject(i).getString("weight"));
							 double upq = 0.0;
							 if(maxQuantity>0){
								 iiaCurrent = overlapQuantity * wgtCurrent / (100 * maxQuantity);
								 iiaTotal = iiaCurrent + iiaTotal;
								 double pop = Double.parseDouble(f.getAttribute("population").toString());
								 if(pop>0){
									 upq = iiaTotal/pop;
								 }
							 }
							 
							 f.setAttribute("iia", iiaTotal);
							 f.setAttribute("iia_"+strAbbr, iiaCurrent);
							 if(enableRankAmongAllLGAs){
								 f.setAttribute("iiag_"+strAbbr, overlapQuantity);
							 }
							 f.setAttribute("wgt_"+strAbbr, wgtCurrent);
							 if(enableUPQ){
								 f.setAttribute("upq", upq);
							 }
						 }
						 fIteratorMB.close();
					}
					
				}
				
			}
			
			
			// create and iia and upq rank within current lga
			List<SimpleFeature> tmpLGAOutputFL = new ArrayList<SimpleFeature>();
			DefaultFeatureCollection tmpLGAOutputFC =  new DefaultFeatureCollection();
			//store iia and upq filled MBs into final output feature collection
			
			fIteratorMB = fcMBTmp.features();
			 while (fIteratorMB.hasNext()) 
		     {
				SimpleFeature ftmp = fIteratorMB.next();
				tmpLGAOutputFL.add(ftmp);
			 }
			 fIteratorMB.close();
			 
			 Collections.sort(tmpLGAOutputFL, new Comparator<SimpleFeature>() {
				    @Override
				    public int compare(SimpleFeature a, SimpleFeature b) {
				            if((double)a.getAttribute("iia") < (double)b.getAttribute("iia")){
				            	return 1;
				            }else if((double)a.getAttribute("iia") == (double)b.getAttribute("iia")){
				            	return 0;
				            }
				            else{
				            	return -1;
				            }
				    }
				});

				int rank_iia = 1;
				int jump_iia = 0;
				for(int i=0; i<tmpLGAOutputFL.size();i++){
					tmpLGAOutputFL.get(i).setAttribute("iia_r", rank_iia);

					//if not the last element
					if(i < tmpLGAOutputFL.size() -1)
					{
						//only increase the rank if current value is NOT equal to the next value
						if(!MathUtils.doubleEquals((double)tmpLGAOutputFL.get(i).getAttribute("iia"), (double)tmpLGAOutputFL.get(i+1).getAttribute("iia"), 0.000000001))
						{
							rank_iia = rank_iia + jump_iia + 1;
							jump_iia = 0;
						}else
						{
							jump_iia++;
						}
					}
				}

				if(enableUPQ){
					//sort upq
					Collections.sort(tmpLGAOutputFL, new Comparator<SimpleFeature>() {
					    @Override
					    public int compare(SimpleFeature a, SimpleFeature b) {
					            if((double)a.getAttribute("upq") < (double)b.getAttribute("upq")){
					            	return 1;
					            }else if((double)a.getAttribute("upq") == (double)b.getAttribute("upq")){
					            	return 0;
					            }
					            else{
					            	return -1;
					            }
					    }
					});
			
					int rank_upq = 1;
					int jump_upq = 0;
					for(int i=0; i<tmpLGAOutputFL.size();i++){
						tmpLGAOutputFL.get(i).setAttribute("upq_r", rank_upq);
			
						//if not the last element
						if(i < tmpLGAOutputFL.size() -1)
						{
							//only increase the rank if current value is NOT equal to the next value
							if(!MathUtils.doubleEquals((double)tmpLGAOutputFL.get(i).getAttribute("upq"), (double)tmpLGAOutputFL.get(i+1).getAttribute("upq"), 0.000000001))
							{
								rank_upq = rank_upq + jump_upq + 1;
								jump_upq = 0;
							}else
							{
								jump_upq++;
							}
						}
					}
				}
				
				//all sorting and ranking done, put it featureCollection
				for(int i=0; i<tmpLGAOutputFL.size();i++){
					outputFL.add(tmpLGAOutputFL.get(i));
				}
		}
		
		//handle ranks among LGAs
		if(enableRankAmongAllLGAs){
			//calc iiag for each MB based on iiag_strAbbr
			for(int i=0; i<outputFL.size(); i++){
				double iiag = 0.0f;
				double upqg = 0.0f;
				for(int j=0; j<layerList.length(); j++){
					
					String strAbbr = layerList.getJSONObject(j).getString("layernameabbr");
					
					 double wgtCurrent = Double.parseDouble(layerList.getJSONObject(j).getString("weight"));
					 double sumMaxQuantity =  Double.parseDouble(layerList.getJSONObject(j).getString("sumMaxQuantity"));
					 if(sumMaxQuantity > 0){
						 double overlapQuantity = Double.parseDouble(outputFL.get(i).getAttribute("iiag_"+strAbbr).toString());
						 iiag = iiag + overlapQuantity * wgtCurrent / (100 * sumMaxQuantity);
					 }	
				}
				outputFL.get(i).setAttribute("iiag", iiag);
				
				if(enableUPQ){
					double pop = Double.parseDouble(outputFL.get(i).getAttribute("population").toString());
					 if(pop>0){
						 upqg = iiag/pop;
					 }
					outputFL.get(i).setAttribute("upqg", upqg);
				}
			}
			
			//then create the rank for iiag and upqg
			Collections.sort(outputFL, new Comparator<SimpleFeature>() {
			    @Override
			    public int compare(SimpleFeature a, SimpleFeature b) {
			            if((double)a.getAttribute("iiag") < (double)b.getAttribute("iiag")){
			            	return 1;
			            }else if((double)a.getAttribute("iiag") == (double)b.getAttribute("iiag")){
			            	return 0;
			            }
			            else{
			            	return -1;
			            }
			    }
			});

			int rank_iiag = 1;
			int jump_iiag = 0;
			for(int i=0; i<outputFL.size();i++){
				outputFL.get(i).setAttribute("iiag_r", rank_iiag);

				//if not the last element
				if(i < outputFL.size() -1)
				{
					//only increase the rank if current value is NOT equal to the next value
					if(!MathUtils.doubleEquals((double)outputFL.get(i).getAttribute("iiag"), (double)outputFL.get(i+1).getAttribute("iiag"), 0.000000001))
					{
						rank_iiag = rank_iiag + jump_iiag + 1;
						jump_iiag = 0;
					}else
					{
						jump_iiag++;
					}
				}
			}

			if(enableUPQ){
				//sort upq
				Collections.sort(outputFL, new Comparator<SimpleFeature>() {
				    @Override
				    public int compare(SimpleFeature a, SimpleFeature b) {
				            if((double)a.getAttribute("upqg") < (double)b.getAttribute("upqg")){
				            	return 1;
				            }else if((double)a.getAttribute("upqg") == (double)b.getAttribute("upqg")){
				            	return 0;
				            }
				            else{
				            	return -1;
				            }
				    }
				});
		
				int rank_upqg = 1;
				int jump_upqg = 0;
				for(int i=0; i<outputFL.size();i++){
					outputFL.get(i).setAttribute("upqg_r", rank_upqg);
		
					//if not the last element
					if(i < outputFL.size() -1)
					{
						//only increase the rank if current value is NOT equal to the next value
						if(!MathUtils.doubleEquals((double)outputFL.get(i).getAttribute("upqg"), (double)outputFL.get(i+1).getAttribute("upqg"), 0.000000001))
						{
							rank_upqg = rank_upqg + jump_upqg + 1;
							jump_upqg = 0;
						}else
						{
							jump_upqg++;
						}
					}
				}
			}
		}
		
		//all sorting and ranking done, put it featureCollection
		for(int i=0; i<outputFL.size();i++){
			outputFC.add(outputFL.get(i));
		}
		
		fIterator.close();
		wfsDS.dispose();

		///////////////////// export shpfile
		
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType, outputFC, new File(outputShpFilePath_iia), true, CRS.decode("EPSG:4326"));
		ShapeFileUtils.featuresExportToShapeFile(outputFeatureType_netbuf, outputFC_netbuf, new File(outputShpFilePath_netbuf), true, CRS.decode("EPSG:4326"));

		///////////////////// prepare data for geoserver publishing
		JSONArray vectors = new JSONArray();
		// add iia layer
		JSONObject vec = new JSONObject();
		vec.put("forcesrs", "EPSG:4326");
		vec.put("nativesrs", "EPSG:4326");
		vec.put("path", outputShpFilePath_iia);
		vec.put("name", simCode+"_iia");
		vec.put("layertype", "polygon");
		vec.put("layerclass", "iia");
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

	public static SimpleFeatureCollection cleanSimpleFeatureCollection(SimpleFeatureCollection fc, File newFile,
			boolean createSchema,  CoordinateReferenceSystem targetCRS){
	
		try {
			
			String surfix = "shp,prj,dbf,shx,fix";
			
			String filename = newFile.getName().replaceAll(".shp", "");
			String filepath = newFile.getAbsolutePath();
			//LOGGER.info("=== {}, {}",filename, filepath);
			ShapeFileUtils.featuresExportToShapeFile(fc.getSchema(), fc, newFile, createSchema, targetCRS);
			SimpleFeatureCollection newfc = ShapeFileUtils.readFeaturesFromShp(newFile, filename);
			DefaultFeatureCollection outputFC =  new DefaultFeatureCollection();
			SimpleFeatureIterator itr = newfc.features();
			while(itr.hasNext()){
				
				outputFC.add(itr.next());
			}
			itr.close();

			LOGGER.info("outputFC size: {}",outputFC.size());
			
			String[] arr = surfix.split(",");
			for(int i=0;i<arr.length; i++)
			{
				String fp = filepath.replaceAll(".shp", "."+arr[i]);
				File f = new File(fp);
				if(f.exists()){
					//LOGGER.info("to be deleted: {}", f.getPath());
					f.delete();
				}
			}
			
			return outputFC;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FactoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}

}
