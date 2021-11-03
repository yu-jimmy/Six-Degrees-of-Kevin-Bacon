package ca.utoronto.utm.mcs;
import static org.neo4j.driver.Values.parameters;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.*;

public class Neo4jDB {
	private Driver driver;
	private String uriDb;
	
	public Neo4jDB() {
		uriDb = "bolt://localhost:7687";
		driver = GraphDatabase.driver(uriDb, AuthTokens.basic("neo4j","1234"));
	}
	
	public void insertActor(String name, String actorId) throws BadRequestException, Exception {
		Session session = driver.session();
		if (isNameInDB(name, "actor") || isIdInDB(actorId, "actor")) {
			throw new BadRequestException("Name and/or Id exists in the database already");
		}
		session.writeTransaction(tx -> tx.run("MERGE (a:actor {name: $x, id: $y})", parameters("x", name, "y", actorId)));
		session.close();
	}
	
	public void insertMovie(String movie, String movieId) throws BadRequestException, Exception {
		Session session = driver.session();
		if (isNameInDB(movie, "movie") || isIdInDB(movieId, "movie")) {
			throw new BadRequestException("Name and/or Id exists in the database already");
		}
		session.writeTransaction(tx -> tx.run("MERGE (a:movie {name: $x, id: $y})", parameters("x", movie, "y", movieId)));
		session.close();
	}
	
	public void insertRelationship(String actorId, String movieId) throws NotFoundException, BadRequestException, Exception {
		Session session = driver.session();
		if(isRelationshipInDB(actorId, movieId)) {
			throw new BadRequestException("Relationship already exists in the database");
		}
		if(!isIdInDB(actorId, "actor") || !isIdInDB(movieId, "movie")) {
			throw new NotFoundException("One or both of the ids do not exist");
		}
		session.writeTransaction(tx -> tx.run("MATCH (a:actor {id:$x}),"
				+ "(t:movie {id:$y})\n" + 
				 "MERGE (a)-[r:ACTED_IN]->(t)\n" + 
				 "RETURN r", parameters("x", actorId, "y", movieId)));
		session.close();
	}
	
	
	public boolean isNameInDB(String name, String type) throws Exception {
		Session session = driver.session();
		Transaction tx = session.beginTransaction();
		Result result = tx.run("MATCH (n:" + type + ") RETURN n.name");
		while(result.hasNext()) {
			Record record = result.next();
			if (name.equals(record.get("n.name").asString())) {
				return true;
			}
		}
		return false;
	}
	
	public boolean isIdInDB(String Id, String type) throws Exception {
		Session session = driver.session();
		Transaction tx = session.beginTransaction();
		Result result = tx.run("MATCH (n:" + type + ") RETURN n.id");
		while(result.hasNext()) {
			Record record = result.next();
			if (Id.equals(record.get("n.id").asString())) {
				return true;
			}
		}
		return false;
	}
	
	public boolean isRelationshipInDB(String actorId, String movieId) throws Exception {
		Session session = driver.session();
		Transaction tx = session.beginTransaction();
		Result result = tx.run("MATCH ({id: $x})-[r]->({id: $y})\n"
				+ "RETURN type(r)", parameters("x", actorId, "y", movieId));
		while(result.hasNext()) {
			Record record = result.next();
			if(record.get(0).asString().equals("ACTED_IN")){
				return true;
			}
		}
		return false;
	}
	
	public String getActor(String actorId) throws JSONException, NotFoundException, BadRequestException, Exception {
		try (Session session = driver.session()){
			try (Transaction tx = session.beginTransaction()){
				
				//ActorName
				Result result = tx.run("MATCH (a:actor {id: $x})\n"
						+ "RETURN a.name", parameters("x", actorId));
				String actorName = "";
				if (result.hasNext()) {
					Record record = result.next();
					actorName = record.get(0).asString();
				} else {
					throw new NotFoundException("ActorId does not exist in database");
				}
				
				
				//Movies
				result = tx.run("MATCH (a:actor {id: $x})-[:ACTED_IN]->(movie)\n"
						+"RETURN movie.id", parameters("x", actorId));
				List<Record> myRecords = result.list();
	
				List<String> movieArray = new ArrayList<String>();
				
				for (Record record: myRecords) {
					movieArray.add(record.get(0).asString());
				}
				
				
				//JSONObject
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("movies", new JSONArray(movieArray));
				jsonObject.put("name", actorName);
				jsonObject.put("actorId", actorId);
				
				return jsonObject.toString();
			}
		}
	}
	
	public String getMovie (String movieId) throws BadRequestException, NotFoundException, JSONException, Exception {
		try (Session session = driver.session()) {
			try (Transaction tx = session.beginTransaction()){
				
				//MovieName
				Result result = tx.run("MATCH (m:movie {id: $x})\n"
						+ "RETURN m.name as movie", parameters("x", movieId));
				String movieName = "";
				if (result.hasNext()) {
					movieName = result.list().get(0).get("movie").asString();
				} else {
					throw new NotFoundException("MovieId does not exist in database");
				}
				
				
				//Actors
				result = tx.run("MATCH (m:movie {id: $x})<-[:ACTED_IN]-(actor)"
						+ "RETURN actor.id", parameters("x", movieId));
				List<Record> myRecords = result.list();
				
				List<String> actorArray = new ArrayList<String>();
				
				for (Record record: myRecords) {
					actorArray.add(record.get(0).asString());
				}
				
				//JSONObject
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("actors", new JSONArray(actorArray));
				jsonObject.put("name", movieName);
				jsonObject.put("movieId", movieId);
				
				return jsonObject.toString();
				
			}
		}
	}
	
	public String hasRelationship(String movieId, String actorId) throws BadRequestException, NotFoundException, JSONException, Exception {
		try (Session session = driver.session()) {
			try (Transaction tx = session.beginTransaction()){
				
				//Check movieId
				Result result = tx.run("MATCH (m:movie {id: $x})"
						+ "RETURN m.name", parameters("x", movieId));
				
				if(!result.hasNext()) {
					throw new NotFoundException("MovieId does not exist in database");
				}
				
				//Check actorId
				result = tx.run("MATCH (a:actor {id: $x})\n"
						+ "RETURN a.name", parameters("x", actorId));

				if (!result.hasNext()) {
					throw new NotFoundException("ActorId does not exist in database");
				}
				
				
				//Check for relationship
				result = tx.run("MATCH (a:actor {id: $x})-[r]->(m:movie {id: $y})"
						+ "RETURN type(r)", parameters("x", actorId, "y", movieId));
				
				boolean hasRelationship = false;
				
				if(result.hasNext()) {
					Record record = result.next();
					if(record.get(0).asString().equals("ACTED_IN")){
						hasRelationship = true;
					}
				} else {
					hasRelationship = false;
				}
				
				//JSONObject
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("actorId", actorId);
				jsonObject.put("movieId", movieId);
				jsonObject.put("hasRelationship", hasRelationship);
				
				return jsonObject.toString();
			}
		}
	}
	
	public String computeBaconNumber(String actorId) throws BadRequestException, NotFoundException, Exception {
		String kevinId = "nm0000102";
		int pathSize = 0;
		//JSONObject
		JSONObject jsonObject = new JSONObject();
		if (actorId.equals(kevinId)) {
			jsonObject.put("baconNumber", "0");
			return jsonObject.toString();
		}
		try (Session session = driver.session()) {
			try (Transaction tx = session.beginTransaction()){
				//Check actorId
				Result result = tx.run("MATCH (a:actor {id: $x})\n"
						+ "RETURN a.name", parameters("x", actorId));
				if (!result.hasNext()) {
					throw new BadRequestException("ActorId does not exist in database");
				}
				
				result = tx.run("MATCH shortestPath=shortestPath((:actor {id: $id1}) - [*] - (:actor {id: $id2}))"
						+ " RETURN length(shortestPath) as size", parameters("id1", actorId, "id2", kevinId));
				if(!result.hasNext()) {
					throw new NotFoundException("Path does not exist");
				}
				
				pathSize = result.list().get(0).get("size").asInt() / 2;
				jsonObject.put("baconNumber", String.valueOf(pathSize));
				return jsonObject.toString();
			}
		}
	}
	
	public String computeBaconPath(String actorId) throws BadRequestException, NotFoundException, Exception {
		String kevinId = "nm0000102";
		int pathSize = 0;
		
		//JSONObject
		JSONObject jsonObject = new JSONObject();
		JSONArray path = new JSONArray();
		JSONObject part = new JSONObject();
		
		try (Session session = driver.session()) {
			try (Transaction tx = session.beginTransaction()) {
				Result result;
				if (actorId.equals(kevinId)) {
					//Movies
					result = tx.run("MATCH (a:actor {id: $x})-[:ACTED_IN]->(movie)\n"
							+"RETURN movie.id", parameters("x", actorId));
					if(!result.hasNext()) {
						throw new NotFoundException("No movie for bacon");
					}
					
					List<Record> myRecords = result.list();
					List<String> movieArray = new ArrayList<String>();
					
					for (Record record: myRecords) {
						movieArray.add(record.get(0).asString());
					}
					
					
					part.put("actorId", kevinId);
					part.put("movieId", movieArray.get(0));
					path.put(part);
					
					jsonObject.put("baconNumber", "0");
					jsonObject.put("baconPath", path);
					
					return jsonObject.toString();
				}
				
				//Check actorId
				result = tx.run("MATCH (a:actor {id: $x})\n"
						+ "RETURN a.name", parameters("x", actorId));
				if (!result.hasNext()) {
					throw new BadRequestException("ActorId does not exist in database");
				}
				
				result = tx.run("MATCH shortestPath=shortestPath((:actor {id: $id1}) - [*] - (:actor {id: $id2}))"
						+ " RETURN length(shortestPath) as size, shortestPath as sp", parameters("id1", actorId, "id2", kevinId));
				if(!result.hasNext()) {
					throw new NotFoundException("Path does not exist");
				}
				List<Record> myRecords = result.list();
				
				pathSize = myRecords.get(0).get("size").asInt() / 2;
				jsonObject.put("baconNumber", String.valueOf(pathSize));
				
				List<String> nodes = new ArrayList<String>();
				List<String> actors = new ArrayList<String>();
				List<String> movies = new ArrayList<String>();
				
				Iterable<Node> iterator = myRecords.get(0).get("sp").asPath().nodes();
				
				for(Node i: iterator) {
					nodes.add(i.get("id").asString());
				}
				
				for(int i = 1; i < nodes.size(); i++) {
					part = new JSONObject();
					if (i % 2 == 0) {
						actors.add(nodes.get(i));
						movies.add(nodes.get(i - 1));
					} else {
						actors.add(nodes.get(i - 1));
						movies.add(nodes.get(i));
					}
				}
				
				for (int i = 0; i < actors.size(); i++) {
					part = new JSONObject();
					part.put("actorId", actors.get(i));
					part.put("movieId", movies.get(i));
					
					path.put(part);
				}
				
				jsonObject.put("baconPath", path);
				return jsonObject.toString();
			}
		}
	}
	
	public void close() {
		driver.close();
	}
}
