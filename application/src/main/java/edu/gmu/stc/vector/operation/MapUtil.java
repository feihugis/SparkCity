package edu.gmu.stc.vector.operation;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Yun Li on 2/28/18.
 */
public class MapUtil {

	public static String localToRemote(String localFile, String hostname, String username, String password,
			String remoteDir) {
		String remotePath = "";
		SSHCommander commander = new SSHCommander(hostname, username, password);
		try {
			remotePath = commander.copyDirToRemote(localFile, remoteDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		commander.closeConnection();

		return remotePath;
	}
	
	public static Map<String, String> getShpInfo(String filepath){

		Map<String, String> mapInfo = new HashMap<>();

		File file = new File(filepath);
		if(!file.exists() || !filepath.endsWith(".shp")) {
			try {
				throw new Exception("Invalid shapefile filepath: " + filepath);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		ShapefileDataStore dataStore = null;
		try {
			dataStore = new ShapefileDataStore(file.toURL());
			ContentFeatureSource featureSource = dataStore.getFeatureSource();

			// get dynamically the CRS of your data:
			SimpleFeatureType schema = featureSource.getSchema();
			CoordinateReferenceSystem sourceCRS = schema.getCoordinateReferenceSystem();
			String code = CRS.lookupIdentifier( sourceCRS, true );
			mapInfo.put("code", code);

			//bbox=984018.1663741902,207673.09513056703,991906.4970533887,219622.53973435296
			ReferencedEnvelope env = featureSource.getBounds();
			String bbox = env.getMinimum(0) + "," + env.getMinimum(1) + "," + env.getMaximum(0) + "," + env.getMaximum(1);
			mapInfo.put("bbox", bbox);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return mapInfo;
	}

	public static String publishWMS(String shpFolder){
		//configaration
		String remoteDir = "/usr/share/geoserver/data_dir/data/";
		String hostname = "199.26.254.146";
		String password = "cisc255b";
		String username = "root";

		File curDir = new File(shpFolder);
		String[] files = curDir.list();
		String shpFileName = "";
		String shpFilePath = "";
		for(String file: files){
			if(file.endsWith(".shp")){
				shpFileName = file;
				shpFilePath = shpFolder + "/" + shpFileName;
			}
		}

		//get projection and boundary box
		Map<String, String> mapInfo = MapUtil.getShpInfo(shpFilePath);
		String code = mapInfo.get("code");
		String bbox = mapInfo.get("bbox");

		//copy folder to remote machine
		String remotePath = MapUtil.localToRemote(shpFolder, hostname, username, password,  remoteDir);

		//publish map
		Geoclient client = new Geoclient();
		String workspace = "mapWorkspace";
		String dataStore = shpFileName + Math.random();
		remotePath = "file:///" + remotePath + shpFileName;
		String datasetName = shpFileName.replace(".shp", "");
		String publishedUrl = client.publishShapefile(workspace, dataStore, datasetName, remotePath, code, bbox);
		//System.out.println(publishedUrl);

		return publishedUrl;
	}

	public static void main( String[] args ) throws IOException
	{
		/***test copy local file to remote ****/
		MapUtil util = new MapUtil();
		String hostname = "199.26.254.146";
		String password = "cisc255b";
		String username = "root";
		String localFile = "D:\\python workspace\\data\\dc-soil-type\\";
		String remoteDir = "/usr/share/geoserver/data_dir/data/";
		//String remotePath = util.localToRemote(localFile, hostname, username, password, remoteDir);
		//System.out.print(remotePath);
		//String remotePath = "/usr/share/geoserver/data_dir/data/dc-soil-type/";
		//util.getShpInfo("D:\\python workspace\\data\\dc-surface\\Impervious_Surface_2015.shp");

		/*
		Geoclient client = new Geoclient();
		String bbox = "-77.11754873964377,38.792343324528275,-76.90946841778161,38.99561050732789";
		String code = "EPSG:4326";
		String workspace = "yun";
		String shpFileName = "Soil_Type_by_Slope";
		remotePath = "file:////usr/share/geoserver/data_dir/data/Soil_Type_by_Slope/Soil_Type_by_Slope.shp";
		String dataStore = shpFileName + (int)Math.random();
		System.out.println(dataStore);
		String publishedUrl = client.publishShapefile(workspace, dataStore, shpFileName, remotePath, code, bbox);

		if(publishedUrl.equals("")){
			System.out.println("can't publish map");
		}else{
			System.out.println(publishedUrl);
		}
*/
		//util.publishWMS("D:\\python workspace\\data\\Soil_Type_by_Slope\\");
		MapUtil.publishWMS("D:\\python workspace\\data\\Soil_Type_by_Slope\\");


		//"http://http://199.26.254.146:8080/geoserver/yun/wms?service=WMS&version=1.1.0&request=GetMap&layers=yun:Soil_Type_by_Slope0.15356805741107682&styles=&bbox=-77.11754873964377,38.792343324528275,-76.90946841778161,38.99561050732789&width=506&height=768&srs=EPSG:4326&format=application/openlayers";
	}
}
