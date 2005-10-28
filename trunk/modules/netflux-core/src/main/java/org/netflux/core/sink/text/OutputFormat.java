package org.netflux.core.sink.text;

import java.text.Format;
import java.util.HashMap;
import java.util.Map;

public class OutputFormat
  {
  private static final long   serialVersionUID = 3824798791601857034L;

  private String              delimiter        = ",";
  private Map<String, Format> formats          = new HashMap<String, Format>( );

  /**
   * @return Returns the delimiter.
   */
  public String getDelimiter( )
    {
    return this.delimiter;
    }

  /**
   * @param delimiter The delimiter to set.
   */
  public void setDelimiter( String delimiter )
    {
    this.delimiter = delimiter;
    }

  /**
   * @return Returns the formats.
   */
  public Map<String, Format> getFormats( )
    {
    return this.formats;
    }

  /**
   * @param formats The formats to set.
   */
  public void setFormats( Map<String, Format> formats )
    {
    this.formats = formats;
    }
  }
