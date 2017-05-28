class Node {
  private int pixel, x, y;
  private String label;
  private List<Edge> neighbours;
  private color nodeColor;
  
  public Node(int pixel, int width, color c) {
    this.pixel = pixel;
    this.nodeColor = c;
    this.x = pixel % width;
    this.y = pixel / width;
    
    this.neighbours = new LinkedList<Edge>();
  }
  
  public void addNeighbour(int n){
    this.neighbours.add(new Edge(this.pixel, n));
  }
  
  public void addNeighbour(int n, int pixelDist){
    this.neighbours.add(new Edge(this.pixel, n, pixelDist));
  }
  
  public List<Edge> getNeighbours(){
    return this.neighbours;
  }
  
  public int getPixel(){
    return this.pixel;
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
}