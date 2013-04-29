package proj.zoie.hourglass.impl;

import proj.zoie.api.DocIDMapper;

public class NullDocIDMapper implements DocIDMapper
{
  public static final NullDocIDMapper INSTANCE = new NullDocIDMapper();
  public int getDocID(long uid)
  {
    return DocIDMapper.NOT_FOUND;
  }

  public int quickGetDocID(long uid)
  {
    return DocIDMapper.NOT_FOUND;
  }  
}
