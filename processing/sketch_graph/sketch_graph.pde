import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Stack;
import java.util.ListIterator;
import java.util.HashSet;
import java.util.Queue;
import java.util.LinkedHashMap;

PImage myImage;
final color GENERAL_GATES = color(0.0, 255.0, 255.0);  // blue
final color ENTRANCE = color(76.0, 255.0, 0.0);        // green
final color RANGER_STOPS = color(255.0, 216.0, 0.0);   // yellow
final color CAMPING = color(255.0, 106.0, 0.0);        // orange
final color GATES = color(255.0, 0.0, 0.0);            // red
final color RANGER_BASE = color(255.0, 0.0, 220.0);    // pink

final color WHITE = color(255.0, 255.0, 255.0);        // white to check non road pixels.

Graph g, sensor;

int scale = 5;
boolean DEBUG = false;

void debugPrint(String s) {
  if(DEBUG) println(s);
}

void debugPrint(boolean cond, String s) {
  if (cond) { debugPrint(s); }
}

void setup() {
  size(1000, 1000);      // scaling to get a bigger graph. multiplying in draw graph function.
  noLoop();
  
  myImage = loadImage("/Users/atuladhar/projects/vastChallenge/lekagulRoadways_roads_only.png");
  myImage.loadPixels();
  
  //Map<Integer, String> landMarks = findSensorPositions();    // Look for sensor locations.
  /** print sensor positions */
  //for (Map.Entry<String, Integer> l : landMarks.entrySet()) {
  //  println(l.getKey() + " is at pixel " + l.getValue());
  //}
  //myImage.updatePixels();
  //myImage.save("/Users/atuladhar/projects/vastChallenge/landMarks.png");
  
  g = createGraph();    // Create the initial graph with all pixel points as nodes.
  g.findLandMarks();    // find landmark nodes in the graph.
  
  // Create a minimized graph with only landmarks as nodes. Also store pixel distance.
  //sensor = createSensorGraphBFS(g);
  sensor = createSensorGraphDFS(g); 
}

void draw() {
  //image(myImage, 0, 0);
  //plotSensorColors();
  
  //Drawing Graph
  background(255);
  //g.draw(scale);
  //sensor.draw(scale);
  //String path = "entrance0:generalGate4:generalGate7:entrance1";
  String path = "entrance0:entrance0:generalGate4:generalGate7:generalGateee:entrance1:entrance1";
  

  //JSONObject obj = new JSONObject("{\"carId\":\"20150022010045-622\",\"carTypes\":\"\",\"date\":\"2015-08-22\",\"totalTime\":2083,\"timedPath\":[{\"_1\":\"2015-08-22 01:35:28\",\"_2\":\"entrance0\",\"_3\":106},{\"_1\":\"2015-08-22 01:33:42\",\"_2\":\"general-gate1\",\"_3\":180},{\"_1\":\"2015-08-22 01:30:42\",\"_2\":\"ranger-stop2\",\"_3\":208},{\"_1\":\"2015-08-22 01:27:14\",\"_2\":\"ranger-stop0\",\"_3\":221},{\"_1\":\"2015-08-22 01:23:33\",\"_2\":\"general-gate2\",\"_3\":1368},{\"_1\":\"2015-08-22 01:00:45\",\"_2\":\"entrance2\",\"_3\":0}]}");
  JSONObject obj = loadJSONObject("/Users/atuladhar/projects/vastChallenge/processing/sketch_graph/test.json");
  //print(obj);
  JSONArray timedPath = obj.getJSONArray("timedPath");
  
  //sensor.drawPath(path, scale);
  sensor.drawPathFromJson(timedPath, scale);
  
  //save("outputs/sensorGraph");
}

/** Create graph representation of the map 
  * Each pixel on the road is represented as a node in the graph.
  */
Graph createGraph() {
  Graph graph = new Graph(myImage);
  
  for (int i = 0; i < 200*200; i++) {
    color currentPixel = myImage.pixels[i];
    
    if(currentPixel != WHITE){
      Node node = new Node(i, graph.getWidth(), currentPixel);
      
      for (Integer n : graph.findNeighbours(i)){
        node.addWeightedNeighbour(new Node(n, graph.getWidth(), myImage.pixels[n]), 0);
      }
      graph.addNamedNode(node);
    }
  }
  return graph;
}

/** Use the result from createGraph (with landmarks) to create a new minimized graph
  * with only landmarks as nodes. Also calculates the pixel distances between the landmarks
  */
Graph createSensorGraphDFS(Graph g){
  Graph sg = new Graph(myImage);
  Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();       // to count pixel distance.
  Map<Integer, List<Integer>> pathMap = new HashMap<Integer, List<Integer>>();    // to track the path of the edge.
  
  int depth = 0;

  for (Map.Entry<String, Node> n : g.namedNodes.entrySet()){
    if(n.getValue().getLabel() != null){
      Node sourceNode = new Node(n.getValue());    // A sensor node
      Node sgNode = new Node(n.getValue());        // Create a new node for the sensor graph
      sgNode.initNeighbours();                     // Initialise neighbours for sensor graph node
      
      for (Edge sourceNeighbor : sourceNode.getNeighbours()) {  // loop over all immediate neighbours of the sensor node.
        Node node = g.namedNodes.get(sourceNeighbor.target.getName());
        Node prev = node;    // track previous node to remove from the neighbour list and check backtracking.
        
        distMap.clear();      // start fresh for each immediate neighbour of sensor node.
        distMap.put(node.getPixel(), 1);    // each immediate neighbour of sensor node is 1 pixel away.
        
        pathMap.clear();      // start fresh for each immediate neighbour of sensor node.
        pathMap.put(node.getPixel(), new LinkedList<Integer>());  
        
        // dfs initialisation
        LinkedList<Node> visited = new LinkedList<Node>();    // Changed from HashSet to LinkedList to remove deeper nodes when backtracking.
        
        Stack<Node> toExplore = new Stack<Node>();    // Track the current node to explore.
        toExplore.push(node);
        visited.add(node);
        
        // Do the search
        while (!toExplore.empty()) {
          Node curr = toExplore.pop();

          if(distMap.get(prev.getPixel()) >= distMap.get(curr.getPixel())){
            depth = distMap.get(prev.getPixel()) - distMap.get(curr.getPixel());
            if(!visited.isEmpty()) visited.subList(visited.size() - depth, visited.size()).clear(); 
          } 
          
          List<Edge> neighbors = curr.getNeighbours();
          
          //remove parent node
          neighbors.remove(prev);
          
          ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
          
          while (it.hasPrevious()) {     //reverse. ???
            Node next = g.namedNodes.get(it.previous().target.getName());
            
            if(next.getLabel() != null && next.getPixel() != sourceNode.getPixel()) {
              distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
              
              List currentPath = new LinkedList<Integer>(pathMap.get(curr.getPixel()));
              pathMap.put(next.getPixel(), currentPath);
              
              sgNode.addWeightedNeighbour(next, distMap.get(next.getPixel()), pathMap.get(next.getPixel()));
            } else if (!visited.contains(next)) {
              distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
              
              List currentPath = new LinkedList<Integer>(pathMap.get(curr.getPixel()));
              currentPath.add(next.getPixel());
              pathMap.put(next.getPixel(), currentPath);
                                        
              visited.add(next);
              toExplore.push(next);
            }
          }
          prev = curr;
        }
        sg.addNamedNode(sgNode);
      }
    }
  }  
  println(sg.toString());
  return sg;
}

/** NOTE: Changed to DFS for counting pixels */
Graph createSensorGraphBFS(Graph g) {
  Graph sg = new Graph(myImage);

  for (Map.Entry<String, Node> n : g.getNamedNodes().entrySet()){
    if(n.getValue().getLabel() != null){
      Node node = new Node(n.getValue());
      Node sgNode = new Node(n.getValue());
      sgNode.initNeighbours();
      
      Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();
      distMap.put(node.getPixel(), 0);
      
      // bfs initialisation
      HashSet<Node> visited = new HashSet<Node>();
      Queue<Node> toExplore = new LinkedList<Node>();
      visited.add(node);
      toExplore.add(node);
            
      // Do the search
      while (!toExplore.isEmpty()) {
        Node curr = toExplore.remove();
     
        List<Edge> neighbors = curr.getNeighbours();
        ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
                
        while (it.hasPrevious()) {     //reverse. ???     
          Node next = g.getNamedNodes().get(it.previous().target.getName());
                    
          if(next.getLabel() != null && next.getPixel() != node.getPixel()) {
            distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
            sgNode.addWeightedNeighbour(next, distMap.get(next.getPixel()));
          } else if (!visited.contains(next)) {
            distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
            visited.add(next);
            toExplore.add(next);
          }  
        }
      }
      sg.addNamedNode(sgNode);
    }
  }  
  println(sg.toString());
  return sg;
}

/** Get the pixel value based on x and y coordinates. 
    @param x -> x-coordinate
    @param y -> y-coordinate
    @returns pixel -> pixel value OR returns NULL for negative coordinates
  */
Integer getPixelValue(Integer x, Integer y, Integer width) {
  if (x < 0 || y < 0) { return null; }
  return (x + y*width);
}

/** Map the types of sensors to pixel position */
Map<Integer, String> findSensorPositions(){
  int ggCount = 0, eCount = 0, rsCount = 0, cCount = 0, gCount = 0;
  //Map<String, Integer> landMarks = new LinkedHashMap<String, Integer>();
  Map<Integer, String> landMarks = new LinkedHashMap<Integer, String>();

  color white = color(255.0, 255.0, 255.0);
  int count = 0;
  
  for (int i = 0; i < 200*200; i++) {
    color currentPixel = myImage.pixels[i];
    if(currentPixel == GENERAL_GATES) {
      landMarks.put(i, "genralGate"+Integer.toString(ggCount++));
    } else if(currentPixel == ENTRANCE) {
      landMarks.put(i, "entrance"+Integer.toString(eCount++));
    } else if(currentPixel == RANGER_STOPS) {
      landMarks.put(i, "rangerStop"+Integer.toString(rsCount++));
    } else if(currentPixel == CAMPING) {
      landMarks.put(i, "camping"+Integer.toString(cCount++));
    } else if(currentPixel == GATES) {
      landMarks.put(i, "gates"+Integer.toString(gCount++));
    } 
    
    /*else if (currentPixel != white) {
      count++;
      println(count + "pixel number color = " + i + " R = " + red(currentPixel) + " G = " + green(currentPixel) + " B = " + blue(currentPixel));
    }*/
  }
  return landMarks;
}

void findSensorColors(){
  float r, g, b;
  
  for (int i = 0; i < 200*200; i++) {
    r = red(myImage.pixels[i]);
    g = green(myImage.pixels[i]);
    b = blue(myImage.pixels[i]);

    if (r == 255.0 && g == 255.0 && b == 255.0){
      myImage.pixels[i] = color(255,255,255,0);
    } else if ((r == 0.0 && g == 0.0 && b == 0.0) || ( r == g && r == b && b == g )){
      myImage.pixels[i] = color(255,255,255,0);
    } else {
      // Print Sensor colors
      println("pixel number = " + i + " R = " + r + " G = " + g + " B = " + b);
    }
  }
}

/** Sensor colors used in the map: 
    1. BLUE = GENERAL GATES   => R = 0.0   G = 255.0 B = 255.0
    2. GREEN = ENTRANCE       => R = 76.0  G = 255.0 B = 0.0
    3. YELLOW = RANGER-STOPS  => R = 255.0 G = 216.0 B = 0.0
    4. ORANGE = CAMPING       => R = 255.0 G = 106.0 B = 0.0
    5. RED = GATES            => R = 255.0 G = 0.0   B = 0.0
  */
void plotSensorColors() {
  background(255);
  
  int length = 20, pos = 1;
  fill(0, 255, 255);
  rect(length * pos, length, length, length);
  
  pos += 1;
  fill(76, 255, 0);
  rect(length * pos, length, length, length);
  
  pos += 1;
  fill(255, 216, 0);
  rect(length * pos, length, length, length);
  
  pos += 1;
  fill(255, 106, 0);
  rect(length * pos, length, length, length);
  
  pos += 1;
  fill(255, 0, 0);
  rect(length * pos, length, length, length);
}