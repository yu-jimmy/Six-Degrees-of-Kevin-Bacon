package ca.utoronto.utm.mcs;

import java.io.IOException;
import java.io.OutputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
public class AddRelationship implements HttpHandler {

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
		} catch (NotFoundException e3) {
			r.sendResponseHeaders(404, -1);
		} catch (Exception e4) {
			r.sendResponseHeaders(500, -1);
		}
	}

	private void handlePut(HttpExchange r) throws Exception, IOException, NotFoundException, BadRequestException, JSONException {
		String body = Utils.convert(r.getRequestBody());
        JSONObject deserialized = new JSONObject(body);

        String actorId = "";
        String movieId = "";

        if (deserialized.has("actorId") && deserialized.has("movieId")) {
        	actorId = deserialized.getString("actorId");
            movieId = deserialized.getString("movieId");
        } else {
   			throw new JSONException("Missing information in JSON");
        }

        Neo4jDB nDB = new Neo4jDB();
        
        nDB.insertRelationship(actorId, movieId);
        
        r.sendResponseHeaders(200, -1);
        
        nDB.close();
	}

}
