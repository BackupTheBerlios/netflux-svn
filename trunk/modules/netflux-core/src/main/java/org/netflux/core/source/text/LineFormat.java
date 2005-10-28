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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * @author jgonzalez
 */
public class LineFormat
  {
  private String            delimiter;
  private Locale            locale;
  private List<FieldFormat> fieldFormats;
  private List<String>      fieldNames;

  /**
   * 
   */
  public LineFormat( )
    {
    this.setDelimiter( " " );
    this.setFieldFormats( new ArrayList<FieldFormat>( ) );
    }

  /**
   * 
   */
  public LineFormat( String delimiter, List<FieldFormat> fieldFormats )
    {
    this.setDelimiter( delimiter );
    this.setFieldFormats( fieldFormats );
    }

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
   * @return Returns the locale.
   */
  public Locale getLocale( )
    {
    return this.locale;
    }

  /**
   * @param locale The locale to set.
   */
  public void setLocale( Locale locale )
    {
    this.locale = locale;
    }

  /**
   * @return Returns the fieldFormats.
   */
  public List<FieldFormat> getFieldFormats( )
    {
    return Collections.unmodifiableList( this.fieldFormats );
    }

  /**
   * @param fieldFormats The fieldFormats to set.
   */
  public void setFieldFormats( List<FieldFormat> fieldFormats )
    {
    this.fieldFormats = new ArrayList<FieldFormat>( fieldFormats );
    this.fieldFormats.removeAll( Collections.singletonList( (FieldFormat) null ) );
    ((ArrayList<FieldFormat>) this.fieldFormats).trimToSize( );

    this.fieldNames = new ArrayList<String>( this.fieldFormats.size( ) );
    for( FieldFormat fieldMetadata : this.fieldFormats )
      {
      this.fieldNames.add( fieldMetadata.getName( ) );
      }
    }

  /**
   * @return
   */
  public int getFieldCount( )
    {
    return this.fieldFormats.size( );
    }

  /**
   * @param fieldName
   * @return
   */
  public int getFieldIndex( String fieldName )
    {
    return this.fieldNames.indexOf( fieldName );
    }
  }
