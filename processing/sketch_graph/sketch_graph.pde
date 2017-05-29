import java.util.*;

PImage myImage;
final color GENERAL_GATES = color(0.0, 255.0, 255.0);  // blue
final color ENTRANCE = color(76.0, 255.0, 0.0);        // green
final color RANGER_STOPS = color(255.0, 216.0, 0.0);   // yellow
final color CAMPING = color(255.0, 106.0, 0.0);        // orange
final color GATES = color(255.0, 0.0, 0.0);            // red

final color WHITE = color(255.0, 255.0, 255.0);        // white to check non road pixels.
//Graph g = createGraph();
Graph g, sensor;
int scale = 5;

void setup() {
  //size(200, 200); 
  //int s = 200*scale;
  size(1000, 1000);      // scaling to get a bigger graph. multiplying in draw graph function.
  noLoop();
  
  myImage = loadImage("/Users/atuladhar/projects/vastChallenge/lekagulRoadways_roads_only.png");
  myImage.loadPixels();
  
  Map<Integer, String> landMarks = findSensorPositions();
  //Graph g = createGraph();
  g = createGraph();
  g.findLandMarks();
  sensor = createSensorGraph(g);
  /** print sensor positions */
  //for (Map.Entry<String, Integer> l : landMarks.entrySet()) {
  //  println(l.getKey() + " is at pixel " + l.getValue());
  //}
  myImage.updatePixels();
  myImage.save("/Users/atuladhar/projects/vastChallenge/landMarks.png");
}

void draw() {
  //image(myImage, 0, 0);
  //plotSensorColors();
  
  //Drawring Graph
  background(255);
  //g.draw(scale);
  sensor.draw(scale);
}

/** Create graph representation of the map */

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

Graph createSensorGraph(Graph g){
  Graph sg = new Graph(myImage);
  int pixelDist = 0;
  Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();
  int breakCount = 0;
  for (Map.Entry<Integer, Node> n : g.nodes.entrySet()){
    //Node node = new Node(n.getValue());
    //if(n.getValue().getLabel() != null){
    if(n.getValue().getLabel() != "DEFAULT"){
      println("\n\n--------------------\n\n");
      breakCount++;
      Node node = new Node(n.getValue());
      Node sgNode = new Node(n.getValue());
      sgNode.initNeighbours();
      
      //sg.addNode(node);
      pixelDist = 0;
      //int pixelDist = 0;
      //Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();
      distMap.put(node.getPixel(), pixelDist);
      
      // dfs init 
      HashSet<Node> visited = new HashSet<Node>();
      Stack<Node> toExplore = new Stack<Node>();
      toExplore.push(node);
            
      // Do the search
      while (!toExplore.empty()) {
        Node curr = toExplore.pop();
        //if(curr.getLabel() != null && curr.getPixel() != node.getPixel()) {
        if(curr.getLabel() != "DEFAULT" && curr.getPixel() != node.getPixel()) {
          //node.addWeightedNeighbour(curr, distMap.get(curr.getPixel()));
          sgNode.addWeightedNeighbour(curr, distMap.get(curr.getPixel()));
          //println("here" + curr.getLabel());
          continue;
        }
        
        List<Edge> neighbors = curr.getNeighbours();
        ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
        //println (curr + " #neigbours = " + neighbors.size());
        while (it.hasPrevious()) {     //reverse. ???
          //Node next = g.nodes.get(it.previous().endPixel);
          //distMap.put(next.getPixel(), pixelDist);
          //Node next = it.previous().target;
          Node next = g.nodes.get(it.previous().target.getPixel());
          
          // debug
          //if (next == null) {println("next is null"); break;}
          
          distMap.put(next.getPixel(), pixelDist);
          
          //println("distmap entry: " + next + " + " + distMap.get(next.getPixel()));
          
          if (!visited.contains(next)) {
            //println("adding node to visited: " + next);
            visited.add(next);
            //parentMap.put(next, curr);
            toExplore.push(next);
          }
        }
        pixelDist++;
        //println("incrementing pixel Dist = " + pixelDist);
      }
      //sg.addNode(node);
      sg.addNode(sgNode);
    }
    //pixelDist++;
    //if (breakCount == 40) break;
  }  
  //println(sg.toString());
  return sg;
}

/*
Graph createSensorGraph(Graph g){
  Graph sg = new Graph(myImage);
  for (Map.Entry<Integer, Node> n : g.nodes.entrySet()){
    Node node = n.getValue();
    if(node.getLabel() != null){
      //sg.addNode(node);
      
      int pixelDist = 0;
      Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();
      distMap.put(node.getPixel(), pixelDist);
      // dfs 
      HashSet<Node> visited = new HashSet<Node>();
      Stack<Node> toExplore = new Stack<Node>();
      toExplore.push(node);
      boolean found = false;
      
      //println("node name: " + node.getLabel());
      
      // Do the search
      while (!toExplore.empty()) {
        Node curr = toExplore.pop();
        if(curr.getLabel() != null && curr.getPixel() != node.getPixel()) {
          //node.addNeighbour(curr.getPixel(), distMap.get(curr.getPixel()));
          node.addWeightedNeighbour(curr, distMap.get(curr.getPixel()));
          //println("node name: " + node.getLabel()+ "    :     " + curr.getPixel(), curr.getLabel());
          continue;
        }
        //if (curr == goal) {
        //  found = true;
        //  break;
        //}
        List<Edge> neighbors = curr.getNeighbours();
        ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
        while (it.hasPrevious()) {     
          Node next = g.nodes.get(it.previous().endPixel);
          distMap.put(next.getPixel(), pixelDist);
          if (!visited.contains(next)) {
            visited.add(next);
            //parentMap.put(next, curr);
            toExplore.push(next);
          }
        }
        pixelDist++;
      }
      sg.addNode(node);
    }
  }  
  println(sg.toString());
  return sg;
}*/

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