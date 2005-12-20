/* 
 * netflux-core - Copyright (C) 2005 OPEN input - http://www.openinput.com/
 *
 * This program is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU General Public License as published by the 
 * Free Software Foundation; either version 2 of the License, or (at your 
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. 
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along 
 * with this program; if not, write to 
 *   the Free Software Foundation, Inc., 
 *   59 Temple Place, Suite 330, 
 *   Boston, MA 02111-1307 USA
 *   
 * $Id$
 */
package org.netflux.core.sink.text;

import java.text.Format;
import java.util.HashMap;
import java.util.Map;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
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
