package com.khattak.bigdata.databases.hbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.json.*;

/*
 * Gets data from the specified table in HBase for the pst 15 minutes only by specifying a scanner filter
 */
public class HbaseRest {	
	
	public static void main(String[] args) throws Exception {
		//Date theTime = new Date(1471422293105L);
		//time.setTime(theTime);
		
		if (args.length != 1){
			System.out.println("Provide the IP & Port of Hbase e.g. 192.168.70.136:9090....");//192.168.70.136:9090
		}
		else {
	        GetData consumption = new GetData(args[0], "consumption"); 
        	new Thread(consumption).start();
        	
        	GetData voltage = new GetData(args[0], "voltage"); 
        	new Thread(voltage).start();
		}
		
		//use below to test without calling hbase
		//String pasrsedResult = parseJsonResultTest();
		//if (!pasrsedResult.isEmpty()) writeFile("C:\\inetpub\\wwwroot\\test\\charts\\city_usage_15_sec.csv", pasrsedResult);
			
	}

	/*
	 * Retrieves voltage data from HBase for the past 15 mins and then writes multiple csv files to "C:\inetpub\wwwroot\test\charts\smart_meter" 
	 */
	private static void getVoltageData(String hbaseLocation) {
		Calendar time = null;
		Long startTime = null;
		Long endTime = null;
		String filteredRecords = null;
		String hbaseScannerEndpoint = "http://" + hbaseLocation + "/smartmeter_voltage/scanner/";
		
		while(true){
			time = Calendar.getInstance();
	        endTime = time.getTimeInMillis();
	        time.add(Calendar.MINUTE, -15);//previous 15 mins
	        startTime = time.getTimeInMillis();
	        
			//String filter = readFilterFile("filter-value-timestamp.txt");
			//String filter = "<Scanner startTime=\"1471424152062\" endTime=\"1471424212245\" />";
        
			String filter = "<Scanner startTime=\""+ startTime +"\" endTime=\""+ endTime +"\" />";
			
			if (filter != null){
				// first get location from where to fetch records
				String scannerLocation = getHbaseScannerLocation(hbaseScannerEndpoint, filter);
				
				// Now get the records
				if(scannerLocation != null && !scannerLocation.isEmpty()) {
					filteredRecords = getFilteredRecords(scannerLocation);
					//System.out.println(filteredRecords);
					
					if (filteredRecords != null){
						List<String> voltage = new ArrayList<String>();
						voltage.add("london");
						voltage.add("birmingham");
						voltage.add("manchester");
						voltage.add("glasgow");
						voltage.add("uk");
						
						String pasrsedResult = parseVoltageJsonResultAll(filteredRecords, voltage);
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\voltage_15_sec.csv", pasrsedResult);
							System.out.println("File voltage_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseVoltageJsonResult(filteredRecords, "london");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\london_voltage_15_sec.csv", pasrsedResult);
							System.out.println("File london_voltage_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseVoltageJsonResult(filteredRecords, "birmingham");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\birmingham_voltage_15_sec.csv", pasrsedResult);
							System.out.println("File birmingham_voltage_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseVoltageJsonResult(filteredRecords, "manchester");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\manchester_voltage_15_sec.csv", pasrsedResult);
							System.out.println("File manchester_voltage_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseVoltageJsonResult(filteredRecords, "glasgow");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\glasgow_voltage_15_sec.csv", pasrsedResult);
							System.out.println("File glasgow_voltage_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseVoltageJsonResult(filteredRecords, "uk");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\uk_voltage_15_sec.csv", pasrsedResult);
							System.out.println("File uk_voltage_15_sec.csv written at: " + (new Date()).toString());
						}
						
						System.out.println("*****");
					}
				}
			}
			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}

	/*
	 * Retrieves consumption data from HBase for the past 15 mins and then writes multiple csv files to "C:\inetpub\wwwroot\test\charts\smart_meter" 
	 */
	private static void getConsumptionData(String hbaseLocation) {
		Calendar time = null;
		Long startTime = null;
		Long endTime = null;
		String filteredRecords = null;
		String hbaseScannerEndpoint = "http://" + hbaseLocation + "/smartmeter_consumption/scanner/";
		while(true){
			time = Calendar.getInstance();
	        endTime = time.getTimeInMillis();
	        time.add(Calendar.MINUTE, -15);//previous 15 mins
	        startTime = time.getTimeInMillis();
	        
			//String filter = readFilterFile("filter-value-timestamp.txt");
			//String filter = "<Scanner startTime=\"1471424152062\" endTime=\"1471424212245\" />";
        
			String filter = "<Scanner startTime=\""+ startTime +"\" endTime=\""+ endTime +"\" />";
			
			if (filter != null){
	
				// first get location from where to fetch records
				String scannerLocation = getHbaseScannerLocation(hbaseScannerEndpoint, filter);
				
				// Now get the actual data
				if(scannerLocation != null && !scannerLocation.isEmpty()) {
					filteredRecords = getFilteredRecords(scannerLocation);
					//System.out.println(filteredRecords);
					
					if (filteredRecords != null){
						List<String> consumption = new ArrayList<String>();
						consumption.add("london");
						consumption.add("birmingham");
						consumption.add("manchester");
						consumption.add("glasgow");
						consumption.add("uk");
						
						String pasrsedResult = parseConsumptionJsonResultAll(filteredRecords, consumption);
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\consumption_15_sec.csv", pasrsedResult);
							System.out.println("File consumption_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseConsumptionJsonResult(filteredRecords, "london");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\london_consumption_15_sec.csv", pasrsedResult);
							System.out.println("File london_consumption_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseConsumptionJsonResult(filteredRecords, "birmingham");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\birmingham_consumption_15_sec.csv", pasrsedResult);
							System.out.println("File birmingham_consumption_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseConsumptionJsonResult(filteredRecords, "manchester");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\manchester_consumption_15_sec.csv", pasrsedResult);
							System.out.println("File manchester_consumption_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseConsumptionJsonResult(filteredRecords, "glasgow");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\glasgow_consumption_15_sec.csv", pasrsedResult);
							System.out.println("File glasgow_consumption_15_sec.csv written at: " + (new Date()).toString());
						}
						
						pasrsedResult = parseConsumptionJsonResult(filteredRecords, "uk");
						if (!pasrsedResult.isEmpty()) {
							writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\uk_consumption_15_sec.csv", pasrsedResult);
							System.out.println("File uk_consumption_15_sec.csv written at: " + (new Date()).toString());
						}
						
						System.out.println("*****");
					}
				}
			}
			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}
	
	/*private static String readFilterFile(String filterFileName) {
		
		String filter = null;
		HbaseRest hbaseRest = new HbaseRest();
		StringBuilder filterSB = new StringBuilder();
		String fileName = "/com/khattak/bigdata/databases/hbase/" + filterFileName;
        
	
		////ClassLoader classLoader = hbaseRest.getClass().getClassLoader();
    	//File file = new File(hbaseRest.getClass().getResource(fileName).getFile());
    	
    	InputStream fileStream = hbaseRest.getClass().getResourceAsStream(fileName);
    	
        String line = null;

        try {
            //FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));

            while((line = bufferedReader.readLine()) != null) {
            	filterSB.append(line);
            }   
            bufferedReader.close();  
            filter = filterSB.toString();
        }
        catch(FileNotFoundException ex) {
            System.out.println("File '" + fileName + "' not found!!!");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '"+ fileName + "'");                  
            ex.printStackTrace();
        }
		return filter;
	}*/

	/*
	 * Gets the URL that will later be used to fetch records
	 */
	private static String getHbaseScannerLocation(String hbaseScannerEndpoint, String filter){
		String scannerLocation = null;
		
		HttpClient client = new HttpClient();
        
        PutMethod putMethod = new PutMethod(hbaseScannerEndpoint);
        StringRequestEntity requestEntity;
		try {
			requestEntity = new StringRequestEntity(filter, "text/xml", null);
			
			putMethod.setRequestHeader("Accept", "text/xml");
	        putMethod.setRequestEntity(requestEntity);
	        
	        int statusCode = client.executeMethod(putMethod);
	        if (statusCode != HttpStatus.SC_CREATED) {
	        	System.err.println("Method failed: " + putMethod.getStatusLine());
	        }
	        else{
	        	scannerLocation = ((Header)putMethod.getResponseHeader("Location")).getValue();
	        	//System.out.println("Scanner location: " + scannerLocation);
	        }
		} catch (UnsupportedEncodingException e) {
			System.out.println("Error while setting request entity!!!");
			e.printStackTrace();
		} catch (HttpException e) {
			System.out.println("Http Exception!!!");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO Exception!!!");
			e.printStackTrace();
		}
        
		
		return scannerLocation;
	}
	
	/*
	 * Gets the JSON-based records from HBase from the passed in scanner location
	 */
	private static String getFilteredRecords(String scannerLocation){
		String results = null;
		
		HttpClient client = new HttpClient();
        int statusCode = 0;
        GetMethod getMethod = new GetMethod(scannerLocation);
        //getMethod.setRequestHeader(new Header("Accept", "text/xml"));
        getMethod.setRequestHeader(new Header("Accept", "application/JSON"));
        try {
			statusCode = client.executeMethod(getMethod);
			
			if (statusCode != HttpStatus.SC_OK) {
	        	System.err.println("Method failed: " + getMethod.getStatusLine());
	        }
			else{
				
		        InputStream rstream = null;
		        
		        rstream = getMethod.getResponseBodyAsStream();
		        
		        BufferedReader br = new BufferedReader(new InputStreamReader(rstream));
		        StringBuilder resultBuilder = new StringBuilder();
		        String result = null;
		        while ((result = br.readLine()) != null) {
		        	resultBuilder.append(result);
		        }
		        br.close();
		        
		        results = resultBuilder.toString();
				}
		} catch (IOException e) {
			System.out.println("IO Exception!!!");
			e.printStackTrace();
		}
		
		return results;
	}
	
	/*
	 * Deciphers json consumption data to a human readable string for the passed in area (city)
	 */
	private static String parseConsumptionJsonResult(String jsonResult, String area){
		StringBuilder parsedResult = new StringBuilder();
        try {  
        	Map<String,String> consumption = new HashMap<String,String>();
        	JSONTokener tokener = new JSONTokener(jsonResult);
        	JSONObject rowsObject =  new JSONObject(tokener);

            JSONArray rows = rowsObject.getJSONArray("Row");
            
            for (int i = 0; i < rows.length(); i++){
            	JSONObject row =  rows.getJSONObject(i);
            	//System.out.println(row.get("key").toString());
            	
            	JSONArray cells = row.getJSONArray("Cell");
            	for (int j = 0; j < cells.length(); j++){
            		JSONObject cell =  cells.getJSONObject(j);
            		String column = new String(Base64.getDecoder().decode(cell.get("column").toString()));
            		
            		if (column.equalsIgnoreCase("timestamp:key")) consumption.put("timestamp", new String(Base64.getDecoder().decode(cell.get("$").toString())));
            		if (column.equalsIgnoreCase(area+":fifteen_second")) consumption.put(area, bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
            	}
            	
        		parsedResult.append(consumption.get("timestamp") + ",");
        		parsedResult.append(consumption.get(area));
            	parsedResult.append(System.getProperty("line.separator"));
            	
            	consumption.clear();
            }
            

            // loop array
            /*JSONArray cars = (JSONArray) jsonObject.get("cars");
            Iterator<String> iterator = cars.iterator();
            while (iterator.hasNext()) {
             System.out.println(iterator.next());
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (!parsedResult.toString().isEmpty()) {
        	parsedResult.insert(0,"timestamp,"+ area);
        	parsedResult.insert(new String("timestamp,"+ area).length(),System.getProperty("line.separator"));
        }
        return parsedResult.toString();
	}
	
	/*
	 * Deciphers json consumption data to a human readable string for the passed in list of areas
	 */
	private static String parseConsumptionJsonResultAll(String jsonResult, List<String> areas){
		StringBuilder parsedResult = new StringBuilder();
        try {  
        	Map<String,String> consumption = new HashMap<String,String>();
        	JSONTokener tokener = new JSONTokener(jsonResult);
        	JSONObject rowsObject =  new JSONObject(tokener);

            JSONArray rows = rowsObject.getJSONArray("Row");
            
            for (int i = 0; i < rows.length(); i++){
            	JSONObject row =  rows.getJSONObject(i);
            	//System.out.println(row.get("key").toString());
            	
            	JSONArray cells = row.getJSONArray("Cell");
            	for (int j = 0; j < cells.length(); j++){
            		JSONObject cell =  cells.getJSONObject(j);
            		String column = new String(Base64.getDecoder().decode(cell.get("column").toString()));
            		
            		switch (column) {
            			case "timestamp:key":consumption.put("timestamp", new String(Base64.getDecoder().decode(cell.get("$").toString())));
						break;
            			case "london:fifteen_second":consumption.put("london", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "birmingham:fifteen_second":consumption.put("birmingham", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "manchester:fifteen_second":consumption.put("manchester", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "glasgow:fifteen_second":consumption.put("glasgow", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "uk:fifteen_second":consumption.put("uk", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			default:;
						break;
            		}
            	}
            	
        		parsedResult.append(consumption.get("timestamp") + ",");
        		
        		if (areas.contains("london")) parsedResult.append(consumption.get("london") + ",");
        		if (areas.contains("birmingham")) parsedResult.append(consumption.get("birmingham") + ",");
        		if (areas.contains("manchester")) parsedResult.append(consumption.get("manchester") + ",");
        		if (areas.contains("glasgow")) parsedResult.append(consumption.get("glasgow") + ",");
        		if (areas.contains("uk")) parsedResult.append(consumption.get("uk"));

            	parsedResult.append(System.getProperty("line.separator"));
            	consumption.clear();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (!parsedResult.toString().isEmpty()) {
        	parsedResult.insert(0,"Timestamp,London,Birmingham,Manchester,Glasgow,UK");
        	parsedResult.insert(new String("Timestamp,London,Birmingham,Manchester,Glasgow,UK").length(),System.getProperty("line.separator"));
        }
        return parsedResult.toString();
	}
	
	/*
	 * Deciphers json voltage data to a human readable string for the passed in area (city)
	 */
	private static String parseVoltageJsonResult(String jsonResult, String area){
		StringBuilder parsedResult = new StringBuilder();
        try {  
        	Map<String,String> consumption = new HashMap<String,String>();
        	JSONTokener tokener = new JSONTokener(jsonResult);
        	JSONObject rowsObject =  new JSONObject(tokener);

            JSONArray rows = rowsObject.getJSONArray("Row");
            
            for (int i = 0; i < rows.length(); i++){
            	JSONObject row =  rows.getJSONObject(i);
            	//System.out.println(row.get("key").toString());
            	
            	JSONArray cells = row.getJSONArray("Cell");
            	for (int j = 0; j < cells.length(); j++){
            		JSONObject cell =  cells.getJSONObject(j);
            		String column = new String(Base64.getDecoder().decode(cell.get("column").toString()));
            		
            		if (column.equalsIgnoreCase("timestamp:key")) consumption.put("timestamp", new String(Base64.getDecoder().decode(cell.get("$").toString())));
            		if (column.equalsIgnoreCase(area+":fifteen_second_min")) consumption.put(area + "_min", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
            		if (column.equalsIgnoreCase(area+":fifteen_second_max")) consumption.put(area + "_max", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
            	}
            	
        		parsedResult.append(consumption.get("timestamp") + ",");
        		parsedResult.append(consumption.get(area + "_min") + ",");
        		parsedResult.append(consumption.get(area + "_max"));
            	parsedResult.append(System.getProperty("line.separator"));
            	
            	consumption.clear();
            }
            

            // loop array
            /*JSONArray cars = (JSONArray) jsonObject.get("cars");
            Iterator<String> iterator = cars.iterator();
            while (iterator.hasNext()) {
             System.out.println(iterator.next());
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (!parsedResult.toString().isEmpty()) {
        	parsedResult.insert(0,"timestamp,"+ area + "_min," +  area + "_max");
        	parsedResult.insert(new String("timestamp,"+ area + "_min," +  area + "_max").length(),System.getProperty("line.separator"));
        }
        return parsedResult.toString();
	}
	
	/*
	 * Deciphers json voltage data to a human readable string for the passed in list of areas
	 */
	private static String parseVoltageJsonResultAll(String jsonResult, List<String> areas){
		StringBuilder parsedResult = new StringBuilder();
        try {  
        	Map<String,String> voltage = new HashMap<String,String>();
        	JSONTokener tokener = new JSONTokener(jsonResult);
        	JSONObject rowsObject =  new JSONObject(tokener);

            JSONArray rows = rowsObject.getJSONArray("Row");
            
            for (int i = 0; i < rows.length(); i++){
            	JSONObject row =  rows.getJSONObject(i);
            	//System.out.println(row.get("key").toString());
            	
            	JSONArray cells = row.getJSONArray("Cell");
            	for (int j = 0; j < cells.length(); j++){
            		JSONObject cell =  cells.getJSONObject(j);
            		String column = new String(Base64.getDecoder().decode(cell.get("column").toString()));
            		
            		switch (column) {
            			case "timestamp:key":voltage.put("timestamp", new String(Base64.getDecoder().decode(cell.get("$").toString())));
						break;
            			case "london:fifteen_second_min":voltage.put("london_min", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "london:fifteen_second_max":voltage.put("london_max", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "birmingham:fifteen_second_min":voltage.put("birmingham_min", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "birmingham:fifteen_second_max":voltage.put("birmingham_max", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "manchester:fifteen_second_min":voltage.put("manchester_min", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "manchester:fifteen_second_max":voltage.put("manchester_max", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "glasgow:fifteen_second_min":voltage.put("glasgow_min", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "glasgow:fifteen_second_max":voltage.put("glasgow_max", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "uk:fifteen_second_min":voltage.put("uk_min", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			case "uk:fifteen_second_max":voltage.put("uk_max", bytesToDouble(Base64.getDecoder().decode(cell.get("$").toString())).toString());
						break;
            			default:;
						break;
            		}
            	}
            	
        		parsedResult.append(voltage.get("timestamp") + ",");
        		
        		if (areas.contains("london")) {
        			parsedResult.append(voltage.get("london_min") + ",");
        			parsedResult.append(voltage.get("london_max") + ",");
        		}
        		if (areas.contains("birmingham")) {
        			parsedResult.append(voltage.get("birmingham_min") + ",");
        			parsedResult.append(voltage.get("birmingham_max") + ",");
        		}
        		if (areas.contains("manchester")) {
        			parsedResult.append(voltage.get("manchester_min") + ",");
        			parsedResult.append(voltage.get("manchester_max") + ",");
        		}
        		if (areas.contains("glasgow")) {
        			parsedResult.append(voltage.get("glasgow_min") + ",");
        			parsedResult.append(voltage.get("glasgow_max") + ",");
        		}
        		if (areas.contains("uk")) {
        			parsedResult.append(voltage.get("uk_min") + ",");
        			parsedResult.append(voltage.get("uk_max"));
        		}

            	parsedResult.append(System.getProperty("line.separator"));
            	voltage.clear();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (!parsedResult.toString().isEmpty()) {
        	parsedResult.insert(0,"Timestamp,London_min,London_max,Birmingham_min,Birmingham_max,Manchester_min,Manchester_max,Glasgow_min,Glasow_max,UK_min,UK_max");
        	parsedResult.insert(new String("Timestamp,London_min,London_max,Birmingham_min,Birmingham_max,Manchester_min,Manchester_max,Glasgow_min,Glasow_max,UK_min,UK_max").length(),System.getProperty("line.separator"));
        }
        return parsedResult.toString();
	}
	
	private static Double bytesToDouble(byte[] bytes) {
    	ByteBuffer doubleBuffer = ByteBuffer.allocate(Double.BYTES);
    	doubleBuffer.put(bytes, 0, bytes.length);
    	doubleBuffer.flip();//need flip 
        return doubleBuffer.getDouble();
    }
	
	private static void writeFile(String fileNameIncludingPath, String data){
		File file = null;
		FileWriter writer = null;
		try {
			file = new File(fileNameIncludingPath);
			if (file.exists()) file.delete();
			file.createNewFile();
			
		    writer = new FileWriter(file, false); 
		    writer.write(data); 
		    writer.flush();
		}
		catch (IOException exp){
			System.out.println("Error while writing hbase results!!!");
		}
		finally{
			try {
				if (writer != null) writer.close();
			}
			catch (IOException exp){
				System.out.println("Error while closing hbase result's file!!!");
			}
		}
	}

	private static class GetData implements Runnable {
		String hbaseLocation = null;
		String dataType = null;
		
		public GetData(String hbaseLocation, String dataType){
			this.hbaseLocation = hbaseLocation;
			this.dataType = dataType;
		}
		
		@Override
		public void run() {
			switch (this.dataType) {
				case "consumption": getConsumptionData(this.hbaseLocation);
				break;
				case "voltage": getVoltageData(this.hbaseLocation);
				break;
				default:;
				break;
			}
			
		}
	}
}
