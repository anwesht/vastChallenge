class Node {
  private int pixel, x, y;
  private String label = "DEFAULT";
  private List<Edge> neighbours;
  private color nodeColor;
  private int width;
  
  public Node(int pixel, int width, color c) {
    this.pixel = pixel;
    this.nodeColor = c;
    this.width = width;
    this.x = pixel % width;
    this.y = pixel / width;
    
    initNeighbours();
  }
  
  public Node(Node copy) {
    this(copy.getPixel(), copy.getWidth(), copy.getNodeColor());
    this.setLabel(copy.getLabel());
    this.neighbours = new LinkedList<Edge>(copy.getNeighbours());
  }
  
  public void addNeighbour(int n){
    this.neighbours.add(new Edge(this.pixel, n));
  }
  
  public void addNeighbour(int n, int pixelDist){
    this.neighbours.add(new Edge(this.pixel, n, pixelDist));
  }
  
  public void addWeightedNeighbour(Node target, int pixelDist){
    this.neighbours.add(new Edge(this, new Node(target), pixelDist));
  }
  
  public void initNeighbours() {
    this.neighbours = new LinkedList<Edge>();
  }
  
  public List<Edge> getNeighbours(){
    return this.neighbours;
  }
  
  public int getPixel(){
    return this.pixel;
  }
  
  public int getWidth(){
    return this.width;
  }
  
  public int getNodeColor(){
    return this.nodeColor;
  }
  
  public void setLabel(String l){
    this.label = l;
  }
  
  public String getLabel() {
    return this.label;
  }
  
  public String toString() {
    String s = "";
    s += getLabel() + " @ ";
    s += "(" + x + ", " + y + ")";
    s += " pixel = " + getPixel();
    s += " R = " + red(getNodeColor()) + " G = " + green(getNodeColor()) + " B = " + blue(getNodeColor());   
    return s;
  }
}