package dev.harritest.model.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Scanner;

import dev.harritest.controller.ContinentJSONSource;

/**
 * 
 * @author farid
 * This class provide default and trivial implementation for api access
 */
public class DefaultContinentJSONSourceImpl implements ContinentJSONSource {

/**
 * need some caching mechanism TODO	
 */
@Override
public String getJSONData() throws IOException {
String jsonStr;
	try(Scanner scan=new Scanner(new URL("https://harri-test-api.herokuapp.com/countries/all").openStream(), "UTF-8")) {
		jsonStr=scan.useDelimiter("\\A").next();
	} catch (MalformedURLException e) {
		e.printStackTrace();
		throw e;
	} catch (IOException e) {
		
		e.printStackTrace();
		throw e;
	}
return 	jsonStr;
}
}
