class Edge {
  String start, end;
  int startPixel, endPixel;
  int sx, sy, ex, ey;
  Integer pixelDistance;
  String direction;
  List<Integer> path;
  
  Node source, target;
  
  public Edge(int s, int e, int pixelDist) {
    this(s, e);
    this.pixelDistance = pixelDist;
  }
  
  public Edge(Node s, Node t, int pixelDist, List<Integer> path) {
    this.source = s;
    this.target = t;
    this.pixelDistance = pixelDist;
    this.direction = s.directionTo(t);
    this.path = new LinkedList<Integer>(path);
  }
  
  public Edge(Node s, Node t, int pixelDist){
    this(s, t, pixelDist, new LinkedList<Integer>());
  }
  
  public Edge(int s, int e) {
    this.startPixel = s;
    this.endPixel = e;
    this.sx = s % width;
    this.sy = s / width;
    this.ex = e % width;
    this.ey = e / width;
    this.pixelDistance = Integer.MIN_VALUE;
  }
  
  @Override
  public boolean equals(Object other){
    if (!(other instanceof Edge)) return false;
    
    Edge e = (Edge) other;
   
    return (this.source.equals(e.source) 
        && this.target.equals(e.target)
        && this.direction.equals(e.direction)
        && this.pixelDistance.equals(e.pixelDistance));
  }
  
  @Override 
  public int hashCode() {
    int hash = pixelDistance.hashCode();
    hash = hash * 31 + source.hashCode();
    hash = hash * 31 + direction.hashCode();
    hash = hash * 31 + target.hashCode();
    return hash;
  }
}