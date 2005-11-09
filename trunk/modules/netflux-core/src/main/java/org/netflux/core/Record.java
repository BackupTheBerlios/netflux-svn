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
package org.netflux.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

// TODO: Improve data manipulation for copies,... right now quite inefficient
// TODO: Handle exceptions !!!! Lots of them uncontrolled
/**
 * @author jgonzalez
 */
public class Record implements Comparable<Record>, Cloneable
  {
  public static Record                  END_OF_DATA = new Record( new RecordMetadata( new LinkedList<FieldMetadata>( ) ) );

  private RecordMetadata                metadata;
  private List<Field<? extends Object>> data;

  /**
   * @param metadata
   */
  public Record( RecordMetadata metadata )
    {
    this( metadata, false );
    }

  /**
   * @param metadata
   */
  public Record( RecordMetadata metadata, boolean nullFields )
    {
    this.metadata = new RecordMetadata( metadata.getFieldMetadata( ) );
    this.data = new ArrayList<Field<? extends Object>>( Collections.nCopies( this.metadata.getFieldCount( ), (Field<Object>) null ) );
    if( nullFields )
      {
      this.nullFields( this.getMetadata( ).getFieldNames( ) );
      }
    }

  /**
   * @return Returns the metadata.
   */
  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  /**
   * @param fieldName
   * @return
   */
  public Field<? extends Object> getField( String fieldName )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    return this.data.get( fieldIndex );
    }

  /**
   * @param fieldName
   * @param field
   */
  public void setField( String fieldName, Field<? extends Object> field )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    this.data.set( fieldIndex, field );
    }

  /**
   * @param record
   */
  public void setFields( Record record )
    {
    for( FieldMetadata currentFieldMetadata : record.getMetadata( ).getFieldMetadata( ) )
      {
      this.setField( currentFieldMetadata.getName( ), record.getField( currentFieldMetadata.getName( ) ) );
      }
    }

  /**
   * @param record
   */
  public void nullFields( Collection<String> fieldNames )
    {
    for( String fieldName : fieldNames )
      {
      this.setValue( fieldName, null );
      }
    }

  /**
   * @param <T>
   * @param clazz
   * @param fieldName
   * @return
   */
  public <T> T getValue( Class<T> clazz, String fieldName )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    if( fieldIndex != -1 )
      {
      Field<? extends Object> field = this.data.get( fieldIndex );
      return (T) field.getValue( );
      }
    else
      {
      throw new IllegalArgumentException( "No field named " + fieldName );
      }
    }

  /**
   * @param <T>
   * @param fieldName
   * @param value
   */
  public <T> void setValue( String fieldName, T value )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    this.data.set( fieldIndex, new Field<T>( value ) );
    }

  /**
   * @param fieldNames
   * @return
   */
  public void remove( Collection<String> fieldNames )
    {
    List<String> fieldsToRemove = new LinkedList<String>( this.metadata.getFieldNames( ) );
    fieldsToRemove.retainAll( fieldNames );

    ListIterator<String> fieldIndexIterator = fieldsToRemove.listIterator( fieldsToRemove.size( ) );
    while( fieldIndexIterator.hasPrevious( ) )
      {
      this.data.remove( fieldIndexIterator.previous( ) );
      }

    this.metadata.remove( fieldNames );
    }

  /**
   * @param fieldNames
   * @return
   */
  public void retain( Collection<String> fieldNames )
    {
    List<String> fieldsToRemove = new LinkedList<String>( this.metadata.getFieldNames( ) );
    fieldsToRemove.removeAll( fieldNames );
    this.remove( fieldsToRemove );
    }

  /**
   * @param record
   */
  public void add( Record record )
    {
    this.metadata.add( record.getMetadata( ) );
    this.data.addAll( record.data );
    }

  /**
   * @param fieldNames
   * @return
   */
  public Record supress( Collection<String> fieldNames )
    {
    List<String> fieldNamesToExtract = new LinkedList<String>( this.metadata.getFieldNames( ) );
    fieldNamesToExtract.removeAll( fieldNames );
    return this.extract( fieldNamesToExtract );
    }

  /**
   * @param fieldNames
   * @return
   */
  public Record extract( List<String> fieldNames )
    {
    Record record = new Record( this.metadata.extract( fieldNames ) );
    for( String fieldName : fieldNames )
      {
      record.setField( fieldName, this.getField( fieldName ) );
      }

    return record;
    }

  /**
   * @param record
   * @return
   */
  public Record concatenate( Record record )
    {
    Record newRecord = new Record( this.getMetadata( ).concatenate( record.getMetadata( ) ) );
    newRecord.data = (List<Field<? extends Object>>) ((ArrayList<Field<? extends Object>>) this.data).clone( );
    newRecord.data.addAll( record.data );
    return newRecord;
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals( Object object )
    {
    return object instanceof Record && this.metadata.equals( ((Record) object).metadata ) && this.data.equals( ((Record) object).data );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode( )
    {
    return this.metadata.hashCode( ) ^ this.data.hashCode( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#clone()
   */
  @Override
  public Object clone( )
    {
    try
      {
      Record clonedRecord = (Record) super.clone( );
      clonedRecord.metadata = (RecordMetadata) this.metadata.clone( );
      clonedRecord.data = (List<Field<? extends Object>>) ((ArrayList<Field<? extends Object>>) this.data).clone( );
      return clonedRecord;
      }
    catch( CloneNotSupportedException exc )
      {
      exc.printStackTrace( );
      throw new InternalError( );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString( )
    {
    StringBuffer recordString = new StringBuffer( "[" );
    for( Field<? extends Object> field : this.data )
      {
      recordString.append( field.toString( ) );
      recordString.append( ',' );
      }
    if( !this.data.isEmpty( ) )
      {
      recordString.deleteCharAt( recordString.length( ) - 1 );
      }
    recordString.append( ']' );
    return recordString.toString( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(T)
   */
  public int compareTo( Record record )
    {
    int result = 0;
    if( this.getMetadata( ).equals( record.getMetadata( ) ) )
      {
      for( int fieldIndex = 0; fieldIndex < this.data.size( ) && result == 0; fieldIndex++ )
        {
        Object currentValue = this.data.get( fieldIndex ).getValue( );
        if( currentValue instanceof Comparable )
          {
          result = ((Comparable<Object>) currentValue).compareTo( record.data.get( fieldIndex ).getValue( ) );
          }
        else
          {
          throw new ClassCastException( "Non comparable data while comparing records" );
          }
        }
      return result;
      }
    else
      {
      throw new ClassCastException( "Incompatible metadata while comparing records" );
      }
    }
  }
