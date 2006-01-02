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

// TODO: Improve javadocs
/**
 * A description of a piece of data contained in a delimited text file.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class FieldFormat
  {
  private String  name;
  private int     type;
  private int     precision;
  private int     scale;
  private String  format;
  private boolean nullable;
  private boolean turningErrorsToNull;

  /**
   * Returns the name of this field.
   * 
   * @return the name of this field.
   */
  public String getName( )
    {
    return this.name;
    }

  /**
   * Sets the name of this field.
   * 
   * @param name the new name of this field.
   */
  public void setName( String name )
    {
    this.name = name;
    }

  /**
   * Returns the type of this field.
   * 
   * @return the type of this field.
   */
  public int getType( )
    {
    return this.type;
    }

  /**
   * Sets the type of this field. The type must be one of the supported types from {@link java.sql.Types}.
   * 
   * @param type the new type of this field.
   */
  public void setType( int type )
    {
    this.type = type;
    }

  /**
   * Returns the precision of this field.
   * 
   * @return the precision of this field.
   */
  public int getPrecision( )
    {
    return this.precision;
    }

  /**
   * Sets the precision of this field.
   * 
   * @param precision the new precision of this field.
   */
  public void setPrecision( int precision )
    {
    this.precision = precision;
    }

  /**
   * Returns the scale of this field.
   * 
   * @return the scale of this field.
   */
  public int getScale( )
    {
    return this.scale;
    }

  /**
   * Sets the scale of this field.
   * 
   * @param scale the new scale of this field.
   */
  public void setScale( int scale )
    {
    this.scale = scale;
    }

  /**
   * Returns the format of this field.
   * 
   * @return the format of this field.
   */
  public String getFormat( )
    {
    return this.format;
    }

  /**
   * Sets the format of this field.
   * 
   * @param format the new format of this field.
   */
  public void setFormat( String format )
    {
    this.format = format;
    }

  /**
   * Returns <code>true</code> if this field may contain <code>null</code>s.
   * 
   * @return <code>true</code> if this field may contain <code>null</code>s.
   */
  public boolean isNullable( )
    {
    return this.nullable;
    }

  /**
   * Sets the nullability of this field.
   * 
   * @param nullable <code>true</code> if this field may contain <code>null</code>s.
   */
  public void setNullable( boolean nullable )
    {
    this.nullable = nullable;
    }

  /**
   * Returns <code>true</code> if a parsing error in this field should be transformed to a <code>null</code> value.
   * 
   * @return <code>true</code> if a parsing error in this field should be transformed to a <code>null</code> value.
   */
  public boolean isTurningErrorsToNull( )
    {
    return this.turningErrorsToNull;
    }

  /**
   * Sets the behavior of this field for parsing errors.
   * 
   * @param turningErrorsToNull <code>true</code> if a parsing error in this field should be transformed to a <code>null</code>
   *          value.
   */
  public void setTurningErrorsToNull( boolean turningErrorsToNull )
    {
    this.turningErrorsToNull = turningErrorsToNull;
    }
  }
