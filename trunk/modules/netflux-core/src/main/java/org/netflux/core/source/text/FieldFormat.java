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
package org.netflux.core.source.text;

/**
 * @author jgonzalez
 */
public class FieldFormat
  {
  private String  name;
  private int     type;
  private int     precision;
  private int     scale;
  private String  format;
  private boolean nullable;

  /**
   * @return
   */
  public String getName( )
    {
    return this.name;
    }

  /**
   * @param name
   */
  public void setName( String name )
    {
    this.name = name;
    }

  /**
   * @return
   */
  public int getType( )
    {
    return this.type;
    }

  /**
   * @param type
   */
  public void setType( int type )
    {
    this.type = type;
    }

  /**
   * @return
   */
  public int getPrecision( )
    {
    return this.precision;
    }

  /**
   * @param precision
   */
  public void setPrecision( int precision )
    {
    this.precision = precision;
    }

  /**
   * @return
   */
  public int getScale( )
    {
    return this.scale;
    }

  /**
   * @param scale
   */
  public void setScale( int scale )
    {
    this.scale = scale;
    }

  /**
   * @return Returns the format.
   */
  public String getFormat( )
    {
    return this.format;
    }

  /**
   * @param format The format to set.
   */
  public void setFormat( String format )
    {
    this.format = format;
    }

  /**
   * @return
   */
  public boolean isNullable( )
    {
    return this.nullable;
    }

  /**
   * @param nullable
   */
  public void setNullable( boolean nullable )
    {
    this.nullable = nullable;
    }
  }
