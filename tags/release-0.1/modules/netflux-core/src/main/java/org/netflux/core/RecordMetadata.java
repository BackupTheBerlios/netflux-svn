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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * A description of the data that may be held in a {@link Record}. This can be conceptually seen as a list of {@link FieldMetadata}
 * instances, each of them describing the data that may be held in each of the fields of a record.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class RecordMetadata implements Serializable, Cloneable
  {
  private static final long              serialVersionUID = -1433565348464531285L;

  private ArrayList<FieldMetadata>       fieldMetadata;
  private LinkedHashMap<String, Integer> fieldIndexes;

  /**
   * Creates a metadata describing a record that can't hold any field.
   */
  public RecordMetadata( )
    {
    this( new ArrayList<FieldMetadata>( ) );
    }

  /**
   * Creates a metadata describing a record that may hold the fields described by the field metadata contained in the supplied list. No
   * null values or duplicated field names are allowed (See {@link RecordMetadata#setFieldMetadata(List)} for more information).
   * 
   * @param fieldMetadata The list of field metadata this record metadata will contain.
   * @throws NullPointerException if <code>fieldMetadata</code> is <code>null</code> or contains a <code>null</code> item.
   * @throws IllegalArgumentException if the list of field metadata contains duplicated field names.
   */
  public RecordMetadata( List<FieldMetadata> fieldMetadata )
    {
    this.setFieldMetadata( fieldMetadata );
    }

  /**
   * Returns the list of field metadata this metadata contains. The returned list is unmodifiable, so if you want to mutate this
   * metadata instance you must use some mutator method instead of trying to directly manipulate the list of field metadata.
   * 
   * @return the list of field metadata this metadata contains.
   */
  public List<FieldMetadata> getFieldMetadata( )
    {
    return Collections.unmodifiableList( this.fieldMetadata );
    }

  /**
   * <p>
   * Sets the list of field metadata that this record metadata will contain. This means that this metadata will describe a record that
   * may contain a list of fields, each of them described by the corresponding field metadata, and the fields will be arranged in the
   * order specified by the list.
   * </p>
   * <p>
   * Passing a <code>null</code> value or including a null field metadata in the list will cause the method to throw an exception. If
   * you want to have a record metadata describing a record that can't hold any field, either use the
   * {@linkplain RecordMetadata#RecordMetadata() default constructor} or pass an empty list to this method.
   * </p>
   * <p>
   * It isn't allowed to have two or more fields in the same record with the same name, so this method will check for duplicated names,
   * and throw an exception in that case.
   * </p>
   * 
   * @param fieldMetadata The list of field metadata this record metadata will contain.
   * @throws NullPointerException if <code>fieldMetadata</code> is <code>null</code> or contains a <code>null</code> item.
   * @throws IllegalArgumentException if the list of field metadata contains duplicated field names.
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
   * Returns the number of fields the record described by this metadata may contain.
   * 
   * @return the number of fields in this metadata.
   */
  public int getFieldCount( )
    {
    return this.fieldMetadata.size( );
    }

  /**
   * Returns the <code>0</code> based position of the field with the supplied <code>name</code> in this metadata. If no field with
   * that <code>name</code> may be found, <code>-1</code> is returned.
   * 
   * @param fieldName the name of the field to be located.
   * @return the position of the field (<code>0</code> based), <code>-1</code> if not found.
   */
  public int getFieldIndex( String fieldName )
    {
    Integer index = this.fieldIndexes.get( fieldName );
    return (index != null) ? index : -1;
    }

  /**
   * Returns a list of the names of the fields described by this metadata. The returned list is not backed by the current list of field
   * metadata, so a change in this metadata won't be reflected in a previously retrieved list.
   * 
   * @return a list of names of the fields described by this metadata.
   */
  public List<String> getFieldNames( )
    {
    return new ArrayList<String>( this.fieldIndexes.keySet( ) );
    }

  /**
   * Returns the metadata associated with the field with the supplied name. If there is no field metadata with the supplied name, an
   * exception is thrown.
   * 
   * @param fieldName the name of the field which metadata we want to get.
   * @return the metadata of the field with the supplied name.
   * @throws NoSuchFieldNameException if no field metadata can be found with the specified name.
   */
  public FieldMetadata getFieldMetadata( String fieldName )
    {
    Integer index = this.fieldIndexes.get( fieldName );
    if( index != null )
      {
      return this.fieldMetadata.get( index );
      }
    else
      {
      throw new NoSuchFieldNameException( "Field metadata not found with name " + fieldName );
      }
    }

  /**
   * Removes from this metadata all the field metadata with names included in the supplied collection.
   * 
   * @param fieldNames the names of the field metadata to remove.
   * @throws NullPointerException if the specified collection is <code>null</code>.
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
   * Retains all the field metadata with names included in the suppled collection. In other words, removes from this metadata all the
   * field metadata with names not included in the supplied collection.
   * 
   * @param fieldNames the names of the field metadata to keep.
   * @throws NullPointerException if the specified collection is <code>null</code>.
   */
  public void retain( Collection<String> fieldNames )
    {
    LinkedHashMap<String, Integer> fieldsToRemove = (LinkedHashMap<String, Integer>) this.fieldIndexes.clone( );
    fieldsToRemove.keySet( ).removeAll( fieldNames );
    this.remove( fieldsToRemove.keySet( ) );
    }

  /**
   * Appends the field metadata contained in the supplied record metadata to the end of the field metadata contained in this metadata.
   * 
   * @param recordMetadata the metadata which field metadata will be appended to this metadata.
   * @throws NullPointerException if <code>recordMetadata</code> is <code>null</code>.
   * @throws IllegalArgumentException if the the supplied record metadata contains some field metadata with the same name that some
   *           field metadata in this metadata.
   */
  public void add( RecordMetadata recordMetadata )
    {
    List<FieldMetadata> newFieldMetadata = new ArrayList<FieldMetadata>( this.getFieldMetadata( ) );
    newFieldMetadata.addAll( recordMetadata.getFieldMetadata( ) );
    this.setFieldMetadata( newFieldMetadata );
    }

  /**
   * Returns a metadata containing all the field metadata of this record metadata with names not included in the supplied collection.
   * 
   * @param fieldNames the names of the field metadata to remove.
   * @return a metadata with the same field metadata that this metadata, supressing the specified field metadata.
   * @throws NullPointerException if the specified collection is <code>null</code>.
   */
  public RecordMetadata supress( Collection<String> fieldNames )
    {
    List<String> fieldNamesToExtract = new LinkedList<String>( this.fieldIndexes.keySet( ) );
    fieldNamesToExtract.removeAll( fieldNames );
    return this.extract( fieldNamesToExtract );
    }

  /**
   * Returns a metadata containing all the field metadata of this record metadata with names included in the supplied list. The order
   * given in the supplied list is preserved in the resulting metadata.
   * 
   * @param fieldNames the names of the field metadata to extract.
   * @return a metadata containing all the field metadata in this metadata which field name is included in the supplied list.
   * @throws NullPointerException if the specified collection is <code>null</code>.
   * @throws IllegalArgumentException if the supplied list contains duplicated names.
   */
  public RecordMetadata extract( List<String> fieldNames )
    {
    List<FieldMetadata> fieldMetadata = new LinkedList<FieldMetadata>( );
    for( String fieldName : fieldNames )
      {
      if( this.getFieldIndex( fieldName ) != -1 )
        {
        fieldMetadata.add( this.getFieldMetadata( fieldName ) );
        }
      }

    return new RecordMetadata( fieldMetadata );
    }

  /**
   * Returns a metadata containing the field metadata of this metadata concatenated with the field metadata contained in the supplied
   * record metadata.
   * 
   * @param recordMetadata the metadata which field metadata will be appended to this metadata.
   * @return a metadata containing the concatenated field metadata of this metadata and the supplied metadata.
   * @throws NullPointerException if <code>recordMetadata</code> is <code>null</code>.
   * @throws IllegalArgumentException if the the supplied record metadata contains some field metadata with the same name that some
   *           field metadata in this metadata.
   */
  public RecordMetadata concatenate( RecordMetadata recordMetadata )
    {
    List<FieldMetadata> newFieldMetadata = new ArrayList<FieldMetadata>( this.getFieldMetadata( ) );
    newFieldMetadata.addAll( recordMetadata.getFieldMetadata( ) );
    return new RecordMetadata( newFieldMetadata );
    }

  /**
   * Compares the specified object with this metadata for equality. Returns <code>true</code> if and only if the specified object is
   * also a record metadata, and the list of field metadata are equal.
   * 
   * @param object The object to be compared for equality with this metadata.
   * @return <code>true</code> if the specified object is equal to this metadata.
   */
  @Override
  public boolean equals( Object object )
    {
    return object instanceof RecordMetadata && this.fieldMetadata.equals( ((RecordMetadata) object).fieldMetadata );
    }

  /**
   * Returns the hash code value for this metadata.
   * 
   * @return the hash code value for this metadata.
   */
  @Override
  public int hashCode( )
    {
    return this.fieldMetadata.hashCode( );
    }

  /**
   * Returns a copy of this metadata. A shallow copy of the private instance variables is done, so changes in the cloned metadata
   * doesn't affect the original instance.
   * 
   * @return a clone of this <code>RecordMetadata</code> instance
   */
  @Override
  public RecordMetadata clone( )
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

  /**
   * Returns a string representation of this record metadata. The string representation is a comma separated list of values surrounded
   * by square brackets.
   * 
   * @return a string representation of this record metadata.
   */
  @Override
  public String toString( )
    {
    StringBuffer metadataString = new StringBuffer( "[" );
    for( FieldMetadata fieldMetadata : this.fieldMetadata )
      {
      metadataString.append( fieldMetadata.toString( ) );
      metadataString.append( ',' );
      }
    if( !this.fieldMetadata.isEmpty( ) )
      {
      metadataString.deleteCharAt( metadataString.length( ) - 1 );
      }
    metadataString.append( ']' );
    return metadataString.toString( );
    }
  }
