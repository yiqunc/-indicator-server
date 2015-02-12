package au.edu.csdila.common;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataSourceException;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.wfs.WFSDataStoreFactory;
import org.geotools.data.wfs.v1_1_0.WFS_1_1_0_DataStore;
import au.edu.csdila.common.AppConfig;


public class WFSDataStore {


	private DataStore dataStoreIDDSS = null;


	public SimpleFeatureSource getFeatureSource(String layerName)throws IOException {
		
		return getDataStore().getFeatureSource(layerName);
	}

	public DataStore getDataStore() throws IOException, DataSourceException {

		if (dataStoreIDDSS == null) {
			Map<String, Object> dataStoreParams = new HashMap<String, Object>();
			String getCapabilities = AppConfig.getString("geoserverRESTURL")+ "/ows?service=wfs&version=1.1.0&request=GetCapabilities";
			dataStoreParams.put("WFSDataStoreFactory:GET_CAPABILITIES_URL",getCapabilities);
			dataStoreParams.put("WFSDataStoreFactory:USERNAME",AppConfig.getString("geoserverRESTUSER"));
			dataStoreParams.put("WFSDataStoreFactory:PASSWORD",AppConfig.getString("geoserverRESTPW"));
			dataStoreParams.put("WFSDataStoreFactory:TIMEOUT", 1000000000);
			dataStoreParams.put("BUFFER_SIZE", 100);
			dataStoreParams.put("WFSDataStoreFactory:WFS_STRATEGY","geoserver");
			dataStoreParams.put("WFSDataStoreFactory:NAMESPACE","http://xxx.unimelb.edu.au");
			dataStoreIDDSS = DataStoreFinder.getDataStore(dataStoreParams);
			
		}
		return dataStoreIDDSS;
	}
	
	public void dispose(){
		
		if(dataStoreIDDSS != null){
			
			dataStoreIDDSS.dispose();
			dataStoreIDDSS = null;
		}
	}

}