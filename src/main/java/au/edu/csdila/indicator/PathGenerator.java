package au.edu.csdila.indicator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.FilterFactory2;

import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.densify.Densifier;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.graph.structure.Node;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.path.AStarShortestPathFinder;
import org.geotools.graph.path.Path;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.traverse.standard.AStarIterator.AStarFunctions;
import org.geotools.graph.traverse.standard.AStarIterator.AStarNode;
public class PathGenerator {
	private static final double LINE_DENSITY = 1;
	private static final double GEOMETRY_PRECISION = 100;
	private static final Double MAX_SNAP_DISTANCE = 500.0;
	private static PrecisionModel precision = new PrecisionModel(GEOMETRY_PRECISION);
	
	static final Logger LOGGER = LoggerFactory.getLogger(PathGenerator.class);
	
	public static List<Path> shortestPaths(LineStringGraphGenerator lineStringGen, List<ODPair> odpairList)
			throws IOException, NoSuchAuthorityCodeException, FactoryException {
	
		
		Graph graph = lineStringGen.getGraph();
		List<Path> paths = new ArrayList<Path>();
		
		for (ODPair od : odpairList) {
			try {
				Node startNode = lineStringGen.getNode(od.oPoint.getCoordinate());
				Node endNode = lineStringGen.getNode(od.dPoint.getCoordinate());
				// if both nodes can be found, 
				if (startNode!=null && endNode != null) {
					Path shortest = findAStarShortestPath(graph, startNode, endNode);
					paths.add(shortest);
				}
				else{
					//if cannot build a path, give an empty one
					Path nullPath = new Path();
					paths.add(nullPath);
				}
			} catch (Exception e) {
				Path nullPath = new Path();
				paths.add(nullPath);
				LOGGER.error("Something bad happened, ignoring " + e.getMessage());
			}
		}
		
		LOGGER.info("Created {} paths", paths.size());
		
		return paths;
	}
	
	public static List<Path> shortestPaths(SimpleFeatureCollection networkSimpleFeatureCollection,
			List<Point> odpointList, List<ODPair> odpairList)
			throws IOException, NoSuchAuthorityCodeException, FactoryException {
	
		
		LOGGER.info("network FEATURES: {}", networkSimpleFeatureCollection.size());
		LOGGER.info("origin-destination pairs: {}", odpairList.size());
		
		List<LineString> lines = nodeIntersections(networkSimpleFeatureCollection);

		// Build a graph with all the destinations connected
		//LOGGER.info("createGraphWithAdditionalNodes starts");
		LineStringGraphGenerator lineStringGen = createGraphWithAdditionalNodes(lines, odpointList);
		Graph graph = lineStringGen.getGraph();

		List<Path> paths = new ArrayList<Path>();
		for (ODPair od : odpairList) {
			try {
				Node startNode = lineStringGen.getNode(od.oPoint.getCoordinate());
				Node endNode = lineStringGen.getNode(od.dPoint.getCoordinate());
				// if both nodes can be found, 
				if (startNode!=null && endNode != null) {
					Path shortest = findAStarShortestPath(graph, startNode, endNode);
					paths.add(shortest);
				}
				else{
					//if cannot build a path, give an empty one
					Path nullPath = new Path();
					paths.add(nullPath);
				}
			} catch (Exception e) {
				Path nullPath = new Path();
				paths.add(nullPath);
				LOGGER.error("Something bad happened, ignoring " + e.getMessage());
			}
		}
		
		LOGGER.info("Created {} paths", paths.size());
		
		return paths;
	}
	
	public static LineStringGraphGenerator getLineStringGraphGenerator(SimpleFeatureCollection networkSimpleFeatureCollection,
			List<Point> odpointList) throws IOException{
		
		LOGGER.info("network FEATURES: {}", networkSimpleFeatureCollection.size());
		
		List<LineString> lines = nodeIntersections(networkSimpleFeatureCollection);

		// Build a graph with all the destinations connected
		LineStringGraphGenerator lineStringGen = createGraphWithAdditionalNodes(lines, odpointList);
		
		return lineStringGen;
	}

	public static List<Path> shortestPaths(SimpleFeatureCollection networkSimpleFeatureCollection,
			Point start, List<Point> destinations)
			throws IOException, NoSuchAuthorityCodeException, FactoryException {
	
		
		LOGGER.info("network FEATURES: {}", networkSimpleFeatureCollection.size());
		LOGGER.info("Destination FEATURES: {}", destinations.size());
		
		List<LineString> lines = nodeIntersections(networkSimpleFeatureCollection);

		// Build a graph with all the destinations connected
		LOGGER.info("createGraphWithAdditionalNodes starts");
		LineStringGraphGenerator lineStringGen = createGraphWithAdditionalNodes(lines, start, destinations);
		Graph graph = lineStringGen.getGraph();

		LOGGER.info("Point: {}", start.toText());
		Node startNode = lineStringGen.getNode(start.getCoordinate());
		LOGGER.info("Start Node {}", startNode.toString());
		List<Path> paths = new ArrayList<Path>();
		for (Point end : destinations) {
			try {
				Node endNode = lineStringGen.getNode(end.getCoordinate());
				// LOGGER.info("End Node: {}", endNode.toString());
				if (endNode != null) {
					Path shortest = findAStarShortestPath(graph, startNode,endNode);
					paths.add(shortest);

				}
			} catch (Exception e) {
				// LOGGER.error("Something bad happened, ignoring "
				// + e.getMessage());
			}
		}
		LOGGER.info("Created {} paths", paths.size());
		return paths;
	}
	
	public static List<Path> shortestPaths(SimpleFeatureSource networkSource,
			Point start, List<Point> destinations, double maxDistance)
			throws IOException, NoSuchAuthorityCodeException, FactoryException {

		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		String geometryPropertyName = networkSource.getSchema().getGeometryDescriptor().getLocalName();

		CoordinateReferenceSystem crs = networkSource.getSchema().getGeometryDescriptor().getCoordinateReferenceSystem();
	    
		SimpleFeatureCollection networkSimpleFeatureCollection = networkSource.getFeatures(ff.dwithin(
				ff.property(geometryPropertyName), 
				ff.literal(start), maxDistance, crs.getCoordinateSystem().getAxis(0).getUnit().toString()));
	
		
		LOGGER.info("network FEATURES: {}", networkSimpleFeatureCollection.size());
		LOGGER.info("Destination FEATURES: {}", destinations.size());
		
		List<LineString> lines = nodeIntersections(networkSimpleFeatureCollection);

		// Build a graph with all the destinations connected
		LOGGER.info("createGraphWithAdditionalNodes starts");
		LineStringGraphGenerator lineStringGen = createGraphWithAdditionalNodes(lines, start, destinations);
		Graph graph = lineStringGen.getGraph();

		LOGGER.info("Point: {}", start.toText());
		Node startNode = lineStringGen.getNode(start.getCoordinate());
		LOGGER.info("Start Node {}", startNode.toString());
		List<Path> paths = new ArrayList<Path>();
		for (Point end : destinations) {
			try {
				Node endNode = lineStringGen.getNode(end.getCoordinate());
				// LOGGER.info("End Node: {}", endNode.toString());
				if (endNode != null) {
					Path shortest = findAStarShortestPath(graph, startNode,endNode);
					paths.add(shortest);

				}
			} catch (Exception e) {
				// LOGGER.error("Something bad happened, ignoring "
				// + e.getMessage());
			}
		}
		LOGGER.info("Created {} paths", paths.size());
		return paths;
	}
	
	private static List<LineString> nodeIntersections(SimpleFeatureCollection nonNodedNetwork) {
		SimpleFeatureIterator networkIterator = nonNodedNetwork.features();
		LOGGER.info("==size of nonNodedNetwork:{}",nonNodedNetwork.size());
		List<LineString> lines = new ArrayList<LineString>();

		while (networkIterator.hasNext()) {
			SimpleFeature edge = networkIterator.next();
			Geometry line = (Geometry) edge.getDefaultGeometry();
			for (int i = 0; i < line.getNumGeometries(); i++) {
				lines.add((LineString) line.getGeometryN(i));
			}
		}
		LOGGER.info("==size of parsed lines:{}",lines.size());
		return nodeIntersections(lines);

	}

	private static List<LineString> nodeIntersections(List<LineString> rawLines) {
		List<LineString> lines = new ArrayList<LineString>();

		GeometryFactory geomFactory = new GeometryFactory(precision);
		Geometry grandMls = geomFactory.buildGeometry(rawLines);
		Point mlsPt = geomFactory.createPoint(grandMls.getCoordinate());
		try {
			if (!(grandMls.isValid())) {
				LOGGER.info("lines not valid?");
			}
			if (!(mlsPt.isValid())) {
				LOGGER.info("Point not valid?");
			}
			
			//LOGGER.info("=== p1");
			Geometry nodedLines = grandMls.union(mlsPt);
			//LOGGER.info("=== p2");
			lines.clear();

			for (int i = 0, n = nodedLines.getNumGeometries(); i < n; i++) {
				Geometry g = nodedLines.getGeometryN(i);
				if (g.isValid()) {
					if (g instanceof LineString) {
						g = (LineString) Densifier.densify(g, LINE_DENSITY);
						lines.add((LineString) g);
					}
				} else {
					LOGGER.info("Found invalid geometry in network, ignoring");
				}
			}
		} catch (TopologyException te) {
			// te.printStackTrace();
			LOGGER.info("Failure while noding network, {}", te.getMessage());
		}
		LOGGER.info("=== after nodeIntersections lines number: {}", lines.size());
		return lines;
	}
	
	private static LineStringGraphGenerator createGraphWithAdditionalNodes(
			List<LineString> sourceLines, List<Point> allAdditionPoints) throws IOException {

		List<LineString> lines = sourceLines;
		
		for (Point endPoint : allAdditionPoints) {
			LocationIndexedLine endConnectedLine = findNearestEdgeLine(
					sourceLines, MAX_SNAP_DISTANCE, endPoint);
			if (endConnectedLine != null) {
				//LOGGER.info("== dst before add connectingline, lines number:{}", lines.size());
				addConnectingLine(endPoint, lines, endConnectedLine);
				//LOGGER.info("== dst after add connectingline, lines number:{}", lines.size());
			} else {
				LOGGER.info("== cannt find line for end point");
			}
		}
		
		// create a linear graph generator
		LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();
		for (LineString line : lines) {
			lineStringGen.add(line);
		}
		LOGGER.info("== before return createGraphWithAdditionalNodes");
		return lineStringGen;

	}
	
	private static LineStringGraphGenerator createGraphWithAdditionalNodes(
			List<LineString> sourceLines, Point startingPoint,
			List<Point> destinations) throws IOException {

		List<LineString> lines = sourceLines;
		LocationIndexedLine startConnectedLine = findNearestEdgeLine(
				sourceLines, MAX_SNAP_DISTANCE, startingPoint);

		if (startConnectedLine != null) {
			LOGGER.info("== sta before add connectingline, lines number:{}", lines.size());
			addConnectingLine(startingPoint, lines, startConnectedLine);
			LOGGER.info("== sta after add connectingline, lines number:{}", lines.size());
		} else {
			LOGGER.info("== cannt find line for start point");
		}

		for (Point endPoint : destinations) {
			LocationIndexedLine endConnectedLine = findNearestEdgeLine(
					sourceLines, MAX_SNAP_DISTANCE, endPoint);
			if (endConnectedLine != null) {
				LOGGER.info("== dst before add connectingline, lines number:{}", lines.size());
				addConnectingLine(endPoint, lines, endConnectedLine);
				LOGGER.info("== dst after add connectingline, lines number:{}", lines.size());
			} else {
				LOGGER.info("== cannt find line for end point");
			}
		}
		//comment by benny
		//nodeIntersections(lines);

		// create a linear graph generator
		LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();
		for (LineString line : lines) {
			lineStringGen.add(line);
		}
		//LOGGER.info("== before return createGraphWithAdditionalNodes");
		return lineStringGen;

	}
	
	private static List<LineString> addConnectingLine(Point newPoint,
			List<LineString> lines, LocationIndexedLine connectedLine) {
		Coordinate pt = newPoint.getCoordinate();
		LinearLocation here = connectedLine.project(pt);
		Coordinate minDistPoint = connectedLine.extractPoint(here);
		LineString lineA = (LineString) connectedLine.extractLine(connectedLine.getStartIndex(), here);
		LineString lineB = (LineString) connectedLine.extractLine(here, connectedLine.getEndIndex());
		LineString originalLine = (LineString) connectedLine.extractLine(connectedLine.getStartIndex(), connectedLine.getEndIndex());

		GeometryFactory geometryFactory = new GeometryFactory(precision);
		LineString newConnectingLine = geometryFactory.createLineString(new Coordinate[] { pt, minDistPoint });

		lines.add(newConnectingLine);
		
		removeLine(lines, originalLine);
		if(lineA!=null && lineA.getLength()>0){lines.add(lineA);}
		if(lineB!=null && lineB.getLength()>0){lines.add(lineB);}
		
		return lines;
	}
	
	private static LocationIndexedLine findNearestEdgeLine(
			List<LineString> lines, Double maxDistance, Point pointOfInterest)
			throws IOException {
		// Build network Graph - within bounds
		SpatialIndex index = createLineStringIndex(lines);

		Coordinate pt = pointOfInterest.getCoordinate();
		Envelope search = new Envelope(pt);
		search.expandBy(maxDistance);

		/*
		 * Query the spatial index for objects within the search envelope. Note
		 * that this just compares the point envelope to the line envelopes so
		 * it is possible that the point is actually more distant than
		 * MAX_SEARCH_DISTANCE from a line.
		 */
		List<LocationIndexedLine> linesIndexed = index.query(search);

		// Initialize the minimum distance found to our maximum acceptable
		// distance plus a little bit
		double minDist = maxDistance;
		Coordinate minDistPoint = null;
		LocationIndexedLine connectedLine = null;

		for (LocationIndexedLine line : linesIndexed) {

			LinearLocation here = line.project(pt);
			Coordinate point = line.extractPoint(here);
			double dist = point.distance(pt);
			if (dist <= minDist) {
				minDist = dist;
				minDistPoint = point;
				connectedLine = line;
			}
		}

		if (minDistPoint != null) {
			LOGGER.debug("{} - snapped by moving {}\n", pt.toString(), minDist);
			return connectedLine;
		}
		return null;
	}
	
	private static void removeLine(List<LineString> lines, Geometry originalLine) {

		for (LineString line : lines) {
			if ((line.equals(originalLine))) {
				lines.remove(line);
				return;
			}
		}
	}

	private static SpatialIndex createLineStringIndex(List<LineString> lines)
			throws IOException {
		SpatialIndex index = new STRtree();

		// Create line string index
		// Just in case: check for null or empty geometry
		for (LineString line : lines) {
			if (line != null) {
				Envelope env = line.getEnvelopeInternal();
				if (!env.isNull()) {
					index.insert(env, new LocationIndexedLine(line));
				}
			}
		}

		return index;
	}

	private static Path findAStarShortestPath(Graph graph, Node start,
			Node destination) throws Exception { // WrongPathException { <---
													// FIXME: WrongPathException
													// is not visible
		// create a cost function and heuristic for A-Star
		// in this case we are using geometry length
		AStarFunctions asf = new AStarFunctions(destination) {

			@Override
			public double cost(AStarNode n1, AStarNode n2) {
				Coordinate coordinate1 = ((Point) n1.getNode().getObject())
						.getCoordinate();
				Coordinate coordinate2 = ((Point) n2.getNode().getObject())
						.getCoordinate();
				return coordinate1.distance(coordinate2);
			}

			@Override
			public double h(Node n) {
				Coordinate coordinate1 = ((Point) n.getObject())
						.getCoordinate();
				Coordinate coordinate2 = ((Point) this.getDest().getObject())
						.getCoordinate();
				return coordinate1.distance(coordinate2);
			}
		};

		AStarShortestPathFinder pf = new AStarShortestPathFinder(graph,destination, start, asf);
		pf.calculate();
		pf.finish();
		return pf.getPath();
	}
}
