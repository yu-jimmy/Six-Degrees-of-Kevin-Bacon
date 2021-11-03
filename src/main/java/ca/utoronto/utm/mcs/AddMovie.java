package ca.utoronto.utm.mcs;

import java.io.IOException;
import java.io.OutputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class AddMovie implements HttpHandler {

	public void handle(HttpExchange r) throws IOException {
        try {
            if (r.getRequestMethod().equals("PUT")) {
                handlePut(r);
            } else {
            	throw new BadRequestException("Invalid Request");
            }
        } catch (BadRequestException e1) {
			r.sendResponseHeaders(400, -1);
		} catch (JSONException e2) {
			r.sendResponseHeaders(400, -1);
		} catch (Exception e3) {
			r.sendResponseHeaders(500, -1);
		}
	}

	private void handlePut(HttpExchange r) throws Exception, IOException, BadRequestException, JSONException {
		String body = Utils.convert(r.getRequestBody());
    	JSONObject deserialized = new JSONObject(body);

    	String name = "";
    	String movieId = "";

   		if (deserialized.has("name") && deserialized.has("movieId")) {
    		name = deserialized.getString("name");
    		movieId = deserialized.getString("movieId");
   		} else {
   			throw new JSONException("Missing information in JSON");
   		}    		

    	Neo4jDB nDB = new Neo4jDB();
    
		nDB.insertMovie(name, movieId);

        r.sendResponseHeaders(200, -1);
    
		nDB.close();
	}
}
