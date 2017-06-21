import java.math.BigDecimal;

class Edge {
  String start, end;
  int startPixel, endPixel;
  int sx, sy, ex, ey;
  Integer pixelDistance;
  Float distance;
  String direction;
  List<Integer> path;
  
  Node source, target;
  
  public Edge(int s, int e, int pixelDist) {
    this(s, e);
    this.pixelDistance = pixelDist;
    this.distance = calculateDistance(pixelDist);
  }
  
  public Edge(Node s, Node t, int pixelDist, List<Integer> path) {
    this.source = s;
    this.target = t;
    this.distance = calculateDistance(pixelDist);
    this.pixelDistance = pixelDist;
    this.direction = s.directionTo(t);
    this.path = new LinkedList<Integer>(path);
  }
  
  public Edge(Node s, Node t, int pixelDist){
    this(s, t, pixelDist, new LinkedList<Integer>());
  }
  
  private Float calculateDistance(int pixel) {
   Float dist = ((float)12/200) * pixel;
   BigDecimal bdDist = new BigDecimal(dist.toString());
   bdDist = bdDist.setScale(2, BigDecimal.ROUND_HALF_UP);
   return bdDist.floatValue();
  }
  
  public Node getTarget() {
    return this.target;
  }
  
  public Edge(int s, int e) {
    this.startPixel = s;
    this.endPixel = e;
    this.sx = s % width;
    this.sy = s / width;
    this.ex = e % width;
    this.ey = e / width;
    this.pixelDistance = Integer.MIN_VALUE;
    this.distance = Float.MIN_VALUE;
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