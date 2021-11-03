package ca.utoronto.utm.mcs;

import java.io.IOException;
import java.io.OutputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class AddActor implements HttpHandler {

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

	private void handlePut(HttpExchange r) throws Exception, BadRequestException, IOException, JSONException {
        String body = Utils.convert(r.getRequestBody());
        JSONObject deserialized = new JSONObject(body);

        String name = "";
       	String actorId = "";

       	if (deserialized.has("name") && deserialized.has("actorId")) {
       		name = deserialized.getString("name");
       		actorId = deserialized.getString("actorId");
       	}
       	else {
       		throw new JSONException("Missing information in JSON");
       	}	
        Neo4jDB nDB = new Neo4jDB();
        
		nDB.insertActor(name, actorId);

	    r.sendResponseHeaders(200, -1);
        
		nDB.close();
	}
}
