package proj.zoie.api;

public interface DocIDMapperFactory {
  DocIDMapper getDocIDMapper(ZoieSegmentReader reader);
}
