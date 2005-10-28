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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * @author jgonzalez
 */
public class RecordMetadata implements Cloneable
  {
  private ArrayList<FieldMetadata>       fieldMetadata;
  private LinkedHashMap<String, Integer> fieldIndexes;

  /**
   * 
   */
  public RecordMetadata( )
    {
    this.fieldMetadata = new ArrayList<FieldMetadata>( );
    this.fieldIndexes = new LinkedHashMap<String, Integer>( );
    }

  /**
   * @param fieldMetadata
   */
  public RecordMetadata( List<FieldMetadata> fieldMetadata )
    {
    this.setFieldMetadata( fieldMetadata );
    }

  /**
   * @return Returns the fieldMetadata.
   */
  public List<FieldMetadata> getFieldMetadata( )
    {
    return Collections.unmodifiableList( this.fieldMetadata );
    }

  /**
   * @param fieldMetadata The fieldMetadata to set.
   */
  public void setFieldMetadata( List<FieldMetadata> fieldMetadata )
    {
    LinkedHashMap<String, Integer> fieldIndexes = new LinkedHashMap<String, Integer>( );
    int index = 0;
    for( FieldMetadata currentFieldMetadata : fieldMetadata )
      {
      fieldIndexes.put( currentFieldMetadata.getName( ), index );
      index++;
      }

    if( fieldMetadata.size( ) == fieldIndexes.size( ) )
      {
      this.fieldMetadata = new ArrayList<FieldMetadata>( fieldMetadata );
      this.fieldIndexes = fieldIndexes;
      }
    else
      {
      // There are duplicated field names
      throw new IllegalArgumentException( "Duplicated field names not allowed in field metadata" );
      }
    }

  /**
   * @return
   */
  public int getFieldCount( )
    {
    return this.fieldMetadata.size( );
    }

  /**
   * @param fieldName
   * @return
   */
  public int getFieldIndex( String fieldName )
    {
    Integer index = this.fieldIndexes.get( fieldName );
    return (index != null) ? index : -1;
    }

  /**
   * @return
   */
  public List<String> getFieldNames( )
    {
    return new ArrayList<String>( this.fieldIndexes.keySet( ) );
    }

  /**
   * @param fieldName
   * @return
   */
  public FieldMetadata getFieldMetadata( String fieldName )
    {
    // TODO: I think it would be more appropiate to launch an exception here...
    Integer index = this.fieldIndexes.get( fieldName );
    return (index != null) ? this.fieldMetadata.get( index ) : null;
    }

  /**
   * @param fieldNames
   * @return
   */
  public void remove( Collection<String> fieldNames )
    {
    LinkedHashMap<String, Integer> fieldsToRemove = (LinkedHashMap<String, Integer>) this.fieldIndexes.clone( );
    fieldsToRemove.keySet( ).retainAll( fieldNames );

    List<FieldMetadata> newFieldMetadata = (List<FieldMetadata>) this.fieldMetadata.clone( );

    ListIterator<Integer> fieldIndexIterator = new ArrayList<Integer>( fieldsToRemove.values( ) )
        .listIterator( fieldsToRemove.size( ) );
    while( fieldIndexIterator.hasPrevious( ) )
      {
      newFieldMetadata.remove( fieldIndexIterator.previous( ) );
      }

    this.setFieldMetadata( newFieldMetadata );
    }

  /**
   * @param fieldNames
   * @return
   */
  public void retain( Collection<String> fieldNames )
    {
    LinkedHashMap<String, Integer> fieldsToRemove = (LinkedHashMap<String, Integer>) this.fieldIndexes.clone( );
    fieldsToRemove.keySet( ).removeAll( fieldNames );
    this.remove( fieldsToRemove.keySet( ) );
    }

  /**
   * @param recordMetadata
   */
  public void add( RecordMetadata recordMetadata )
    {
    List<FieldMetadata> newFieldMetadata = new ArrayList<FieldMetadata>( this.getFieldMetadata( ) );
    newFieldMetadata.addAll( recordMetadata.getFieldMetadata( ) );
    this.setFieldMetadata( newFieldMetadata );
    }

  /**
   * @param fieldNames
   * @return
   */
  public RecordMetadata supress( Collection<String> fieldNames )
    {
    List<String> fieldNamesToExtract = new LinkedList<String>( this.fieldIndexes.keySet( ) );
    fieldNamesToExtract.removeAll( fieldNames );
    return this.extract( fieldNamesToExtract );
    }

  /**
   * @param fieldNames
   * @return
   */
  public RecordMetadata extract( List<String> fieldNames )
    {
    List<FieldMetadata> fieldMetadata = new LinkedList<FieldMetadata>( );
    for( String fieldName : fieldNames )
      {
      fieldMetadata.add( this.getFieldMetadata( fieldName ) );
      }

    return new RecordMetadata( fieldMetadata );
    }

  /**
   * @param recordMetadata
   * @return
   */
  public RecordMetadata concatenate( RecordMetadata recordMetadata )
    {
    List<FieldMetadata> newFieldMetadata = new ArrayList<FieldMetadata>( this.getFieldMetadata( ) );
    newFieldMetadata.addAll( recordMetadata.getFieldMetadata( ) );
    return new RecordMetadata( newFieldMetadata );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals( Object object )
    {
    return object instanceof RecordMetadata && this.fieldMetadata.equals( ((RecordMetadata) object).fieldMetadata );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode( )
    {
    return this.fieldMetadata.hashCode( );
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
      RecordMetadata clonedRecordMetadata = (RecordMetadata) super.clone( );
      clonedRecordMetadata.fieldMetadata = (ArrayList<FieldMetadata>) this.fieldMetadata.clone( );
      clonedRecordMetadata.fieldIndexes = (LinkedHashMap<String, Integer>) this.fieldIndexes.clone( );
      return clonedRecordMetadata;
      }
    catch( CloneNotSupportedException e )
      {
      e.printStackTrace( );
      throw new InternalError( );
      }
    }
  }
