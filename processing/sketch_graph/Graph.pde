class Graph {
  final color WHITE = color(255.0, 255.0, 255.0); 
  private int width;
  private Map<String, Integer> sensorCounts;
  //private Map<Integer, Node> nodes;
  private Map<String, Node> namedNodes;
  PImage mapImage;
  
  public Graph(PImage img) {
    this.mapImage = img;
    this.width = img.width;
    //this.nodes = new LinkedHashMap<Integer, Node>();
    this.namedNodes = new LinkedHashMap<String, Node>();
    this.sensorCounts = new LinkedHashMap<String, Integer>();
  }    
  
  public int getWidth(){
    return this.width;
  }
  
  public int addSensor(String s){
    int count = sensorCounts.containsKey(s) ? sensorCounts.get(s): 0;
    sensorCounts.put(s, count+1);
    return count;
  }
  
  /** Return list of neighbours. 
      tl | t | tr
      l  | p | r
      bl | b | br
    */
  public List<Integer> findNeighbours(int p) {
    int x = p % this.width;
    int y = p / this.width;
    
    List neighbours = new LinkedList<Integer>();
    
    //addNeighbour(x-1, y-1, neighbours);    // tl
    addNeighbour(x, y-1, neighbours);      // t
    //addNeighbour(x+1, y-1, neighbours);    // tr
      
    addNeighbour(x-1, y, neighbours);      // l
    addNeighbour(x+1, y, neighbours);      // r
    
    //addNeighbour(x-1, y+1, neighbours);    // bl
    addNeighbour(x, y+1, neighbours);      // b
    //addNeighbour(x+1, y+1, neighbours);    // br
    
    return neighbours;
  }
  
  private void addNeighbour(Integer x, Integer y, List neighbours) {
    if (x < 0 || y < 0 ) { return; }
    
    Integer p = x + y*this.width; 
    if(mapImage.pixels[p] != WHITE) {
      neighbours.add(p);
    }
  }
  
  //private void addNode(Node n) {
  //  this.nodes.put(n.getPixel(), n);
  //}
  
  private void addNamedNode(Node n) {
    //this.namedNodes.put(n.getLabel() != null ? n.getLabel() : Integer.toString(n.getPixel()), n);
    this.namedNodes.put(n.getName(), n);
  }
  
  //public Map<Integer, Node> getNodes(){
  //  return this.nodes;
  //}
  public Map<String, Node> getNamedNodes() {
    return this.namedNodes;
  }
  
  //todo : Name nodes.
  public void findLandMarks(){
    //for (Map.Entry<Integer, Node> n : this.nodes.entrySet()){
    for (Map.Entry<String, Node> n : this.getNamedNodes().entrySet()){
      Node node = n.getValue();
      if(node.getNodeColor() == GENERAL_GATES) {
        node.setLabel("generalGate"+Integer.toString(addSensor("generalGate")));
      } else if(node.getNodeColor() == ENTRANCE) {
        node.setLabel("entrance"+Integer.toString(addSensor("entrance")));
      } else if(node.getNodeColor() == RANGER_STOPS) {
        node.setLabel("rangerStop"+Integer.toString(addSensor("rangerStop")));
      } else if(node.getNodeColor() == CAMPING) {
        node.setLabel("camping"+Integer.toString(addSensor("camping")));
      } else if(node.getNodeColor() == GATES) {
        node.setLabel("gates"+Integer.toString(addSensor("gates")));
      } 
      
      //debug
      //if(node.getLabel() != null){
      //  println("node name: " + node.getLabel());
      //  println("map node name: " + this.nodes.get(n.getKey()).getLabel());
      //}
    }
  }
  
/*  void draw(int scale) {
    for (Map.Entry<Integer, Node> n : this.nodes.entrySet()){
      Node node = n.getValue();
      if(node.getLabel() != null){
        fill(node.getNodeColor());
        ellipse(node.x * scale, node.y * scale, 5, 5);
        text(node.getLabel(), node.x * scale + 6, node.y * scale + 6);
        
        //println("node name: " + node.getLabel());

        // Draw Edges.
        for(Edge e: node.getNeighbours()){
          fill(color(0,0,0));
          //text(nodes.get(e.endPixel).getLabel(), node.x * scale + 6, node.y * scale - 6);
          //println("neighbours: " + e.endPixel);

          //line(e.sx * scale, e.sy * scale, e.ex * scale, e.ey * scale);
          //line(node.x * scale, node.y * scale, e.ex * scale, e.ey * scale);
        }
      }
    }  
  }
  */
  
  void draw(int scale) {
    //for (Map.Entry<Integer, Node> n : this.nodes.entrySet()){
    for (Map.Entry<String, Node> n : this.getNamedNodes().entrySet()){
      Node node = n.getValue();
      fill(node.getNodeColor());
      ellipse(node.x * scale, node.y * scale, 5, 5);
      if(node.getLabel() != null){
        text(node.getLabel(), node.x * scale + 6, node.y * scale + 6);
      }
      
      // Draw Edges.
      for(Edge e: node.getNeighbours()){
        fill(color(0,0,0));
        //line(e.source.x * scale, e.source.y * scale, e.target.x * scale, e.target.y * scale);
        
        if(e.path.isEmpty()){
          line(e.source.x * scale, e.source.y * scale, e.target.x * scale, e.target.y * scale);
        } else {
          
          for(Integer i : e.path) {
            int x = i % this.width;
            int y = i / this.width;
            ellipse(x * scale, y * scale, 2, 2);
          }
        }
        
        // debug: Print the target node as triangle and label it as well. 
        /*
        fill(e.target.getNodeColor());
        triangle(e.target.x * scale, e.target.y * scale, e.target.x * scale + 3, e.target.y * scale + 3, e.target.x * scale - 3, e.target.y * scale + 3);
        text(e.target.getLabel(), e.target.x * scale + 6, e.target.y * scale + 6);
        */
      }
    }  
  }
  
  /** Returns adjacency-list representation of graph */
  public String toString() {
    String s = "";
    //for (Map.Entry<Integer, Node> n : this.nodes.entrySet()){
    for (Map.Entry<String, Node> n : this.getNamedNodes().entrySet()){
      Node node = n.getValue();
      s += node.getLabel() + " (" + node.getNeighbours().size() + ")" + " -> ";
      for (Edge e: node.getNeighbours()){
        if(e.target == null){ println("target null"); break; }
        s += e.target.getLabel();
        s += " (" + e.pixelDistance + "), ";
      }
      s += "\n";
    }
    return s;
  }
  
  public List<Node> dfs(Node start, Node goal) {
    if (start == null || goal == null) {
      System.out.println("Start or goal node is null!  No path exists.");
      return new LinkedList<Node>();
    }

    HashMap<Node, Node> parentMap = new HashMap<Node, Node>();
    boolean found = dfsSearch(start, goal, parentMap);
    
    if (!found) {
      System.out.println("No path exists");
      return new LinkedList<Node>();
    }

    // reconstruct the path
    return constructPath(start, goal, parentMap);
  }
  
  private List<Node> constructPath(Node start, Node goal,
      HashMap<Node, Node> parentMap) {
    LinkedList<Node> path = new LinkedList<Node>();
    Node curr = goal;
    while (curr != start) {
      path.addFirst(curr);
      curr = parentMap.get(curr);
    }
    path.addFirst(start);
    return path;
  }

  private boolean dfsSearch(Node start, Node goal, 
      HashMap<Node, Node> parentMap) 
  {
    HashSet<Node> visited = new HashSet<Node>();
    Stack<Node> toExplore = new Stack<Node>();
    toExplore.push(start);
    boolean found = false;

    // Do the search
    while (!toExplore.empty()) {
      Node curr = toExplore.pop();
      if(curr.getLabel() != null ) {
        
      }
      //if (curr == goal) {
      //  found = true;
      //  break;
      //}
      List<Edge> neighbors = curr.getNeighbours();
      ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
      while (it.hasPrevious()) {     
        //Node next = nodes.get(it.previous().endPixel);
        Node next = getNamedNodes().get(it.previous().endPixel);
        if (!visited.contains(next)) {
          visited.add(next);
          parentMap.put(next, curr);
          toExplore.push(next);
        }
      }
    }
    return found;
  }
/*
  private boolean dfsSearch(Node start, Node goal, 
      HashMap<Node, Node> parentMap) {
    HashSet<Node> visited = new HashSet<Node>();
    Stack<Node> toExplore = new Stack<Node>();
    toExplore.push(start);
    boolean found = false;

    // Do the search
    while (!toExplore.empty()) {
      Node curr = toExplore.pop();
      if (curr == goal) {
        found = true;
        break;
      }
      List<Edge> neighbors = curr.getNeighbours();
      ListIterator<Edge> it = neighbors.listIterator(neighbors.size());
      while (it.hasPrevious()) {     
        Node next = nodes.get(it.previous().endPixel);
        if (!visited.contains(next)) {
          visited.add(next);
          parentMap.put(next, curr);
          toExplore.push(next);
        }
      }
    }
    return found;
  } */
}