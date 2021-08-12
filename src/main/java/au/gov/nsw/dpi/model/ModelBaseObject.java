package au.gov.nsw.dpi.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ModelBaseObject {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	@Override
	public String toString() {
		return gson.toJson(this);
	}
}
