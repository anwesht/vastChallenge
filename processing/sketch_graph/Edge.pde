class Edge {
  String start, end;
  int startPixel, endPixel;
  int sx, sy, ex, ey;
  Integer pixelDistance;
  
  Node source, target;
  
  public Edge(int s, int e, int pixelDist) {
    this(s, e);
    this.pixelDistance = pixelDist;
  }
  
  public Edge(Node s, Node t, int pixelDist) {
    this.source = s;
    this.target = t;
    this.pixelDistance = pixelDist;
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
}