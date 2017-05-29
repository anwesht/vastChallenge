class Node {
  private int pixel, x, y;
  //private String label = "DEFAULT";
  private String label;
  private List<Edge> neighbours;
  private color nodeColor;
  private int width;
  
  public Node(int pixel, int width, color c) {
    this.pixel = pixel;
    this.nodeColor = c;
    this.width = width;
    this.x = pixel % width;
    this.y = pixel / width;
    this.label = null;
    
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
  
  @Override
  public String toString() {
    String s = "";
    s += getLabel() + " @ ";
    s += "(" + x + ", " + y + ")";
    s += " pixel = " + getPixel();
    s += " R = " + red(getNodeColor()) + " G = " + green(getNodeColor()) + " B = " + blue(getNodeColor());   
    return s;
  }
  
  @Override 
  public boolean equals(Object other) {
    if (!(other instanceof Node)) return false;
    Node n = (Node) other;
    return (this.getPixel() == n.getPixel()
           && this.x == n.x 
           && this.y == n.y 
           && this.label.equals(n.label));
  }
  
  @Override
  public int hashCode(){
    int hash = pixel;
    hash = hash * 31 + x;
    hash = hash * 31 + y;
    hash = hash * 31 + (getLabel() != null ? getLabel().hashCode() : "DEFAULT".hashCode());
    return hash;
  }
}