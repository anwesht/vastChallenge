import java.util.*;

PImage myImage;
final color GENERAL_GATES = color(0.0, 255.0, 255.0);  // blue
final color ENTRANCE = color(76.0, 255.0, 0.0);        // green
final color RANGER_STOPS = color(255.0, 216.0, 0.0);   // yellow
final color CAMPING = color(255.0, 106.0, 0.0);        // orange
final color GATES = color(255.0, 0.0, 0.0);            // red

final color WHITE = color(255.0, 255.0, 255.0);        // white to check non road pixels.
Graph g, sensor;

int scale = 5;
boolean DEBUG = true;

void debugPrint(String s) {
  if(DEBUG) println(s);
}

void debugPrint(boolean cond, String s) {
  if (cond) { debugPrint(s); }
}

void setup() {
  //size(200, 200); 
  //int s = 200*scale;
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
  //sensor = createSensorGraph(g);    // Create a minimized graph with only landmarks as nodes. Also store pixel distance.
  sensor = createSensorGraphBFS(g);
  //sensor = createSensorGraphDFS(g);
}
/* DFS result
generalGate6 (14) -> generalGate5 (42), generalGate3 (230), camping2 (334), generalGate0 (402), generalGate2 (365), gates3 (336), camping1 (384), entrance2 (193), gates4 (127), gates5 (14), camping8 (66), gates8 (60), entrance4 (61), camping7 (85), 
camping7 (1) -> generalGate6 (85), 
*/
/*BFS result
generalGate6 (14) -> gates5 (14), generalGate5 (42), gates8 (60), entrance4 (61), camping8 (66), gates4 (67), camping7 (85), entrance2 (133), generalGate3 (170), camping2 (274), gates3 (276), generalGate2 (305), camping1 (324), generalGate0 (340), 
camping7 (1) -> generalGate6 (85), */
void draw() {
  //image(myImage, 0, 0);
  //plotSensorColors();
  
  //Drawing Graph
  background(255);
  //g.draw(scale);
  sensor.draw(scale);
  save("graph_representation_bfs");
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
        //node.addNeighbour(n);
        node.addWeightedNeighbour(new Node(n, graph.getWidth(), myImage.pixels[n]), 0);
      }
      
      graph.addNode(node);
    }
  }
  return graph;
}

Graph createSensorGraphBFS(Graph g) {
  Graph sg = new Graph(myImage);
  //int pixelDist = 0;
  Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();

  for (Map.Entry<Integer, Node> n : g.nodes.entrySet()){
    
    //if(n.getValue().getLabel() != null && (n.getValue().getLabel().equals("rangerStop5") 
    //     || n.getValue().getLabel().equals("gates4"))){ 
    
    //if(n.getValue().getLabel() != null && n.getValue().getLabel().equals("generalGate6")){
    if(n.getValue().getLabel() != null && (n.getValue().getLabel().equals("camping7") 
         || n.getValue().getLabel().equals("generalGate6"))){ 
    //if(n.getValue().getLabel() != null){
      Node node = new Node(n.getValue());
      Node sgNode = new Node(n.getValue());
      sgNode.initNeighbours();
      
      debugPrint("\n\nSource node = " + node + "\n\n");
      
      //pixelDist = 1;
      distMap.put(node.getPixel(), 0);
      
      // bfs initialisation
      HashSet<Node> visited = new HashSet<Node>();
      Queue<Node> toExplore = new LinkedList<Node>();
      visited.add(node);
      toExplore.add(node);
      
      HashSet<Edge> visitedEdges = new HashSet<Edge>();
            
      // Do the search
      while (!toExplore.isEmpty()) {
        Node curr = toExplore.remove();
        // if current node is a sensor and is not the source node then add to neigbour of source.
        if(curr.getLabel() != null && curr.getPixel() != node.getPixel()) {
          sgNode.addWeightedNeighbour(curr, distMap.get(curr.getPixel()));
          debugPrint(sgNode + "\n####Adding: " + curr + distMap.get(curr.getPixel()));
          continue;
        }
        
        List<Edge> neighbors = curr.getNeighbours();
        ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
                
        while (it.hasPrevious()) {     //reverse. ???
          Edge e = it.previous();
          //Node next = g.nodes.get(it.previous().target.getPixel());          
          Node next = g.nodes.get(e.target.getPixel());
                    
          // todo: fix for multiple paths between nodes. 
          //if (!visited.contains(next)) {
          //  distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
            
          //  debugPrint(curr + " Adding :" + next + " pixelDistance is:: " + distMap.get(next.getPixel()));
                        
          //  visited.add(next);
          //  toExplore.add(next);
          //}  
          
          if (!visitedEdges.contains(e)) {
            distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
            
            debugPrint(curr + " Adding :" + next + " pixelDistance is:: " + distMap.get(next.getPixel()));
                        
            visitedEdges.add(e);
            toExplore.add(next);
          }  
        }
        debugPrint("End of current node: " + curr + "\n\n");
        //pixelDist++;
        //debugPrint("\n\npixelDistance = " + pixelDist);
      }
      debugPrint("End of loop");
      sg.addNode(sgNode);
    }
    //pixelDist++;
  }  
  println(sg.toString());
  return sg;
}


//Graph createSensorGraphBFS(Graph g) {
//  Graph sg = new Graph(myImage);
//  //int pixelDist = 0;
//  Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();

//  for (Map.Entry<Integer, Node> n : g.nodes.entrySet()){
    
//    //if(n.getValue().getLabel() != null && (n.getValue().getLabel().equals("rangerStop5") 
//    //     || n.getValue().getLabel().equals("gates4"))){ 
    
//    //if(n.getValue().getLabel() != null && n.getValue().getLabel().equals("generalGate6")){
//    if(n.getValue().getLabel() != null && (n.getValue().getLabel().equals("camping7") 
//         || n.getValue().getLabel().equals("generalGate6"))){ 
//    //if(n.getValue().getLabel() != null){
//      Node node = new Node(n.getValue());
//      Node sgNode = new Node(n.getValue());
//      sgNode.initNeighbours();
      
//      debugPrint("\n\nSource node = " + node + "\n\n");
      
//      //pixelDist = 1;
//      distMap.put(node.getPixel(), 0);
      
//      // bfs initialisation
//      HashSet<Node> visited = new HashSet<Node>();
//      Queue<Node> toExplore = new LinkedList<Node>();
//      visited.add(node);
//      toExplore.add(node);
            
//      // Do the search
//      while (!toExplore.isEmpty()) {
//        Node curr = toExplore.remove();
//        // if current node is a sensor and is not the source node then add to neigbour of source.
//        if(curr.getLabel() != null && curr.getPixel() != node.getPixel()) {
//          sgNode.addWeightedNeighbour(curr, distMap.get(curr.getPixel()));
//          debugPrint(sgNode + "\n####Adding: " + curr + distMap.get(curr.getPixel()));
//          continue;
//        }
        
//        List<Edge> neighbors = curr.getNeighbours();
//        ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
                
//        while (it.hasPrevious()) {     //reverse. ???
//          Node next = g.nodes.get(it.previous().target.getPixel());          
                    
//          // Quick fix for multiple paths between nodes. 
//          if (!visited.contains(next)) {
//            distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
            
//            debugPrint(curr + " Adding :" + next + " pixelDistance is:: " + distMap.get(next.getPixel()));
            
//            visited.add(next);
//            toExplore.add(next);
//          }  
//        }
//        debugPrint("End of current node: " + curr + "\n\n");
//        //pixelDist++;
//        //debugPrint("\n\npixelDistance = " + pixelDist);
//      }
//      debugPrint("End of loop");
//      sg.addNode(sgNode);
//    }
//    //pixelDist++;
//  }  
//  println(sg.toString());
//  return sg;
//}


/** NOTE: Changed to BFS for counting pixels */
/** Use the result from createGraph (with landmarks) to create a new minimized graph
  * with only landmarks as nodes. Also calculates the pixel distances between the landmarks
  */
Graph createSensorGraphDFS(Graph g){
  Graph sg = new Graph(myImage);
  //int pixelDist = 0;
  Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();

  for (Map.Entry<Integer, Node> n : g.nodes.entrySet()){
    //if(n.getValue().getLabel() == "rangerStop5" || n.getValue().getLabel() == "gates4"){ 
    if(n.getValue().getLabel() != null && (n.getValue().getLabel().equals("camping7") 
         || n.getValue().getLabel().equals("generalGate6"))){ 
    //if(n.getValue().getLabel() != null){
      Node node = new Node(n.getValue());
      Node sgNode = new Node(n.getValue());
      sgNode.initNeighbours();
      
      //pixelDist = 0;
      distMap.put(node.getPixel(), 0);
      
      // dfs initialisation
      HashSet<Node> visited = new HashSet<Node>();
      Stack<Node> toExplore = new Stack<Node>();
      toExplore.push(node);
      visited.add(node);
            
      // Do the search
      while (!toExplore.empty()) {
        //println("HERERE!!!!!!!");
        Node curr = toExplore.pop();
        // if current node is a sensor and is not the source node then add to neigbour of source.
        //if(curr.getLabel() != null && curr.getPixel() != node.getPixel()) {
        //if(curr.getLabel() != null && !curr.equals(node)) {
        //if(curr.getLabel() != null) {
        if (curr.getLabel() != null && curr.getPixel() != node.getPixel()){
          //println("Here not adding neigbours for node" + curr);
          sgNode.addWeightedNeighbour(curr, distMap.get(curr.getPixel()));
          continue;
        }
        
        List<Edge> neighbors = curr.getNeighbours();
        ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
                
        while (it.hasPrevious()) {     //reverse. ???
          Node next = g.nodes.get(it.previous().target.getPixel());          
          //distMap.put(next.getPixel(), pixelDist);
                    
          if (!(visited.contains(next)) || next.getLabel() != null) {
            distMap.put(next.getPixel(), distMap.get(curr.getPixel()) + 1);
            visited.add(next);
            toExplore.push(next);
          }
        }
        //pixelDist++;
      }
      sg.addNode(sgNode);
    }
    //pixelDist++;
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