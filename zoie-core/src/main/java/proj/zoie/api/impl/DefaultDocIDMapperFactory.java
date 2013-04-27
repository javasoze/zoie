package proj.zoie.api.impl;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieSegmentReader;

public class DefaultDocIDMapperFactory implements DocIDMapperFactory {
	public DocIDMapper getDocIDMapper(ZoieSegmentReader zoieReader) {
		return new DocIDMapperImpl(zoieReader.getUidValues(), zoieReader.maxDoc());
		
	}	
}
