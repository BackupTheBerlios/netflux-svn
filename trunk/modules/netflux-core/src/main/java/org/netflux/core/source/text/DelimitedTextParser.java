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

import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.source.SourceDataStorage;
import org.netflux.core.source.SourceDataStorageException;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class DelimitedTextParser implements SourceDataStorage
  {
  private Scanner        lineScanner;
  private LineFormat     lineFormat;
  private RecordMetadata metadata;

  /**
   * 
   */
  public DelimitedTextParser( )
    {}

  /**
   * @param reader
   * @param delimiter
   */
  public DelimitedTextParser( Reader reader, LineFormat lineFormat )
    {
    this.setReader( reader );
    this.setLineFormat( lineFormat );
    }

  /**
   * @param reader
   */
  public void setReader( Reader reader )
    {
    if( this.lineScanner != null )
      {
      this.lineScanner.close( );
      }

    this.lineScanner = new Scanner( reader );
    }

  /**
   * @param lineFormat The lineFormat to set.
   */
  public void setLineFormat( LineFormat lineFormat )
    {
    this.lineFormat = lineFormat;

    List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>( this.lineFormat.getFieldCount( ) );
    for( FieldFormat fieldFormat : this.lineFormat.getFieldFormats( ) )
      {
      FieldMetadata currentFieldMetadata = new FieldMetadata( );
      currentFieldMetadata.setName( fieldFormat.getName( ) );
      currentFieldMetadata.setType( fieldFormat.getType( ) );
      currentFieldMetadata.setNullable( fieldFormat.isNullable( ) );
      currentFieldMetadata.setPrecision( fieldFormat.getPrecision( ) );
      currentFieldMetadata.setScale( fieldFormat.getScale( ) );

      fieldMetadata.add( currentFieldMetadata );
      }

    this.metadata = new RecordMetadata( fieldMetadata );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.source.SourceDataStorage#getMetadata()
   */
  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.source.SourceDataStorage#nextRecord()
   */
  public Record nextRecord( ) throws SourceDataStorageException
    {
    if( this.lineScanner == null )
      {
      throw new IllegalStateException( );
      }
    else if( this.lineScanner.hasNextLine( ) )
      {
      String currentLine = this.lineScanner.nextLine( );
      Record currentRecord = new Record( this.metadata );

      Scanner fieldScanner = new Scanner( currentLine );
      fieldScanner.useDelimiter( this.lineFormat.getDelimiter( ) );
      fieldScanner.useLocale( this.lineFormat.getLocale( ) );

      for( FieldFormat fieldFormat : this.lineFormat.getFieldFormats( ) )
        {
        this.populateField( currentLine, currentRecord, fieldScanner, fieldFormat );
        }

      return currentRecord;
      }
    else
      {
      return Record.END_OF_DATA;
      }
    }

  /**
   * @param line
   * @param record
   * @param fieldScanner
   * @param fieldFormat
   * @throws SourceDataStorageException
   */
  protected void populateField( String line, Record record, Scanner fieldScanner, FieldFormat fieldFormat )
      throws SourceDataStorageException
    {
    if( this.hasNextField( line, record, fieldScanner, fieldFormat ) )
      {
      this.populateCheckedField( line, record, fieldScanner, fieldFormat );
      }
    else
      {
      try
        {
        if( fieldScanner.next( ).length( ) == 0 )
          {
          if( fieldFormat.isNullable( ) )
            {
            this.nullCurrentField( line, record, fieldFormat );
            }
          else
            {
            throw new SourceDataStorageException( "Null value in non nullable field " + fieldFormat.getName( ), record, line );
            }
          }
        else
          {
          if( fieldFormat.isTurningErrorsToNull( ) && fieldFormat.isNullable( ) )
            {
            this.nullCurrentField( line, record, fieldFormat );
            }
          else
            {
            throw new SourceDataStorageException( "Wrong type trying to assign field " + fieldFormat.getName( ) + " from input ["
                + fieldScanner.match( ).group( ) + "]", record, line );
            }
          }
        }
      catch( NoSuchElementException exc )
        {
        if( fieldScanner.ioException( ) != null )
          {
          throw new SourceDataStorageException( "IOException while parsing input", record, line, true, fieldScanner.ioException( ) );
          }
        else if( this.lineFormat.getFieldFormats( ).indexOf( fieldFormat ) == this.lineFormat.getFieldCount( ) - 1 )
          {
          if( fieldFormat.isNullable( ) )
            {
            this.nullCurrentField( line, record, fieldFormat );
            }
          else
            {
            throw new SourceDataStorageException( "Null value in non nullable field " + fieldFormat.getName( ), record, line );
            }
          }
        else
          {
          if( fieldFormat.isNullable( ) )
            {
            this.nullCurrentField( line, record, fieldFormat );
            }
          else
            {
            throw new SourceDataStorageException( "No more input while trying to assign field " + fieldFormat.getName( ), record, line );
            }
          }
        }
      }
    }

  /**
   * @param line
   * @param record
   * @param fieldScanner
   * @param fieldFormat
   * @return
   * @throws SourceDataStorageException
   */
  protected boolean hasNextField( String line, Record record, Scanner fieldScanner, FieldFormat fieldFormat )
      throws SourceDataStorageException
    {
    switch( fieldFormat.getType( ) )
      {
      case Types.CHAR:
      case Types.VARCHAR:
        return fieldScanner.hasNext( );

      case Types.DATE:
      case Types.TIMESTAMP:
        // We can't know this, so we just return if there is a string available
        return fieldScanner.hasNext( );

      case Types.SMALLINT:
      case Types.INTEGER:
        return fieldScanner.hasNextInt( );

      case Types.BIGINT:
        return fieldScanner.hasNextBigInteger( );

      case Types.DECIMAL:
        return fieldScanner.hasNextBigDecimal( );

      case Types.FLOAT:
        return fieldScanner.hasNextFloat( );

      case Types.DOUBLE:
        return fieldScanner.hasNextDouble( );

      case Types.BOOLEAN:
        return fieldScanner.hasNextBoolean( );

      default:
        throw new SourceDataStorageException( "Type not supported: " + fieldFormat.getType( ), record, line );
      }
    }

  /**
   * @param line
   * @param record
   * @param fieldScanner
   * @param fieldFormat
   * @throws SourceDataStorageException
   */
  protected void populateCheckedField( String line, Record record, Scanner fieldScanner, FieldFormat fieldFormat )
      throws SourceDataStorageException
    {
    switch( fieldFormat.getType( ) )
      {
      case Types.CHAR:
      case Types.VARCHAR:
        // TODO: Should we interpret a zero length string as null? I guess so...
        String fieldValue = fieldScanner.next( );
        if( fieldValue.length( ) != 0 || fieldFormat.isNullable( ) )
          {
          record.setValue( fieldFormat.getName( ), (fieldValue.length( ) != 0) ? fieldValue : null );
          }
        else
          {
          throw new SourceDataStorageException( "Null value in non nullable field " + fieldFormat.getName( ), record, line );
          }
        break;

      case Types.DATE:
      case Types.TIMESTAMP:
        fieldValue = fieldScanner.next( );
        if( fieldValue.length( ) != 0 )
          {
          try
            {
            SimpleDateFormat dateParser = new SimpleDateFormat( fieldFormat.getFormat( ) );
            record.setValue( fieldFormat.getName( ), dateParser.parse( fieldValue ) );
            }
          catch( ParseException exc )
            {
            if( fieldFormat.isTurningErrorsToNull( ) && fieldFormat.isNullable( ) )
              {
              this.nullCurrentField( line, record, fieldFormat );
              }
            else
              {
              throw new SourceDataStorageException( "Wrong type trying to assign field " + fieldFormat.getName( ) + " from input ["
                  + fieldScanner.match( ).group( ) + "]", record, line, exc );
              }
            }
          }
        else
          {
          if( fieldFormat.isNullable( ) )
            {
            record.setValue( fieldFormat.getName( ), (Date) null );
            }
          else
            {
            throw new SourceDataStorageException( "Null value in non nullable field " + fieldFormat.getName( ), record, line );
            }
          }
        break;

      case Types.SMALLINT:
      case Types.INTEGER:
        // TODO: Check if the int type is able to handle SQL INTEGER type
        record.setValue( fieldFormat.getName( ), new Integer( fieldScanner.nextInt( ) ) );
        break;

      case Types.BIGINT:
        // TODO: Should I take into account precision and scale?
        record.setValue( fieldFormat.getName( ), fieldScanner.nextBigInteger( ) );
        break;

      case Types.DECIMAL:
        record.setValue( fieldFormat.getName( ), fieldScanner.nextBigDecimal( ) );
        break;

      case Types.FLOAT:
        record.setValue( fieldFormat.getName( ), new Float( fieldScanner.nextFloat( ) ) );
        break;

      case Types.DOUBLE:
        record.setValue( fieldFormat.getName( ), new Double( fieldScanner.nextDouble( ) ) );
        break;

      case Types.BOOLEAN:
        record.setValue( fieldFormat.getName( ), new Boolean( fieldScanner.nextBoolean( ) ) );
        break;

      default:
        throw new SourceDataStorageException( "Type not supported: " + fieldFormat.getType( ), record, line );
      }
    }

  /**
   * @param line
   * @param record
   * @param fieldFormat
   * @throws SourceDataStorageException
   */
  protected void nullCurrentField( String line, Record record, FieldFormat fieldFormat ) throws SourceDataStorageException
    {
    switch( fieldFormat.getType( ) )
      {
      case Types.CHAR:
      case Types.VARCHAR:
        record.setValue( fieldFormat.getName( ), (String) null );
        break;

      case Types.DATE:
      case Types.TIMESTAMP:
        record.setValue( fieldFormat.getName( ), (Date) null );
        break;

      case Types.SMALLINT:
      case Types.INTEGER:
        record.setValue( fieldFormat.getName( ), (Integer) null );
        break;

      case Types.BIGINT:
        record.setValue( fieldFormat.getName( ), (BigInteger) null );
        break;

      case Types.DECIMAL:
        record.setValue( fieldFormat.getName( ), (BigDecimal) null );
        break;

      case Types.FLOAT:
        record.setValue( fieldFormat.getName( ), (Float) null );
        break;

      case Types.DOUBLE:
        record.setValue( fieldFormat.getName( ), (Double) null );
        break;

      case Types.BOOLEAN:
        record.setValue( fieldFormat.getName( ), (Boolean) null );
        break;

      default:
        throw new SourceDataStorageException( "Type not supported: " + fieldFormat.getType( ), record, line );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.source.SourceDataStorage#close()
   */
  public void close( )
    {
    if( this.lineScanner != null )
      {
      this.lineScanner.close( );
      }
    }
  }
