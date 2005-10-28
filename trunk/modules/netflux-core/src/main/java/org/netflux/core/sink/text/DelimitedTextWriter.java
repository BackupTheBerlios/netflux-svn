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

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Date;

import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.sink.TargetDataStorage;

/**
 * @author jgonzalez
 */
public class DelimitedTextWriter implements TargetDataStorage
  {
  private Writer       writer;
  private OutputFormat outputFormat = new OutputFormat( );

  /**
   * 
   */
  public DelimitedTextWriter( )
    {}

  /**
   * @param reader
   * @param delimiter
   */
  public DelimitedTextWriter( Writer writer, OutputFormat outputFormat )
    {
    this.setWriter( writer );
    this.setOutputFormat( outputFormat );
    }

  /**
   * @param reader
   */
  public void setWriter( Writer writer )
    {
    if( this.writer != null )
      {
      try
        {
        this.writer.close( );
        }
      catch( IOException exc )
        {
        // TODO Auto-generated catch block
        exc.printStackTrace( );
        }
      }

    this.writer = writer;
    }

  /**
   * @return
   */
  public OutputFormat getOutputFormat( )
    {
    return this.outputFormat;
    }

  /**
   * @param outputFormat The outputFormat to set.
   */
  public void setOutputFormat( OutputFormat outputFormat )
    {
    this.outputFormat = outputFormat;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.sink.TargetDataStorage#storeRecord(org.netflux.core.Record)
   */
  public void storeRecord( Record record ) throws IOException
    {
    int currentFieldIndex = 0;
    for( FieldMetadata fieldMetadata : record.getMetadata( ).getFieldMetadata( ) )
      {
      if( record.getField( fieldMetadata.getName( ) ) == null )
        {
        this.writer.write( "<MISSING>" );
        }
      else if( record.getValue( Object.class, fieldMetadata.getName( ) ) != null )
        {
        switch( fieldMetadata.getType( ) )
          {
          case Types.CHAR:
          case Types.VARCHAR:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), String.class ) );
            break;

          case Types.DATE:
          case Types.TIMESTAMP:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), Date.class ) );
            break;

          case Types.SMALLINT:
          case Types.INTEGER:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), Integer.class ) );
            break;

          case Types.BIGINT:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), BigInteger.class ) );
            break;

          case Types.DECIMAL:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), BigDecimal.class ) );
            break;

          case Types.FLOAT:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), Float.class ) );
            break;

          case Types.DOUBLE:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), Double.class ) );
            break;

          case Types.BOOLEAN:
            this.writer.write( this.formatField( record, fieldMetadata.getName( ), Boolean.class ) );
            break;

          default:
          // TODO
          }
        }

      currentFieldIndex++;
      if( currentFieldIndex < record.getMetadata( ).getFieldCount( ) )
        {
        this.writer.write( this.outputFormat.getDelimiter( ) );
        }
      else if( currentFieldIndex == record.getMetadata( ).getFieldCount( ) )
        {
        this.writer.write( System.getProperty( "line.separator" ) );
        }
      }
    }

  /**
   * @param <T>
   * @param record
   * @param fieldName
   * @param clazz
   * @return
   */
  protected <T> String formatField( Record record, String fieldName, Class<T> clazz )
    {
    String formattedField = null;

    if( outputFormat.getFormats( ).containsKey( fieldName ) )
      {
      formattedField = outputFormat.getFormats( ).get( fieldName ).format( record.getValue( clazz, fieldName ) );
      }
    else
      {
      formattedField = record.getValue( clazz, fieldName ).toString( );
      }

    return formattedField;
    }

  /**
   * 
   */
  public void close( )
    {
    if( this.writer != null )
      {
      try
        {
        this.writer.close( );
        }
      catch( IOException exc )
        {
        // TODO Auto-generated catch block
        exc.printStackTrace( );
        }
      }
    }
  }
