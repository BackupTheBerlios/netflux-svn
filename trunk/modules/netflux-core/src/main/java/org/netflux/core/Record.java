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
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * A collection of related items of information ({@link Field}s) treated as a unit. A <code>Record</code> consists of a
 * {@link org.netflux.core.RecordMetadata} describing the data this <code>Record</code> can handle, and a list of {@link Field}s
 * containing the real data.
 * </p>
 * <p>
 * A <code>Record</code> must always be created supplying the corresponding metadata, describing the data this record may handle.
 * Once created you may think of a record as an ordered list of holes where you can place the data allowed by the corresponding field
 * metadata. These holes are initially empty (<code>null</code>), indicating that no piece of data has yet been placed in the
 * record. You may place a {@link Field} instance in any of these holes, even a field with a <code>null</code> value. In this case,
 * you can differentiate a unassigned hole and an assigned hole with a <code>null</code> value.
 * </p>
 * <p>
 * In order to provide a way to signal the end of data, a special constant record is provided: {@link Record#END_OF_DATA}. This is
 * just a record that has an empty metadata, that is, it can't hold any data in it.
 * </p>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class Record implements Comparable<Record>, Serializable, Cloneable
  {
  private static final long                   serialVersionUID = -5058506696243955825L;

  /**
   * Constant record used to signal end of data in a communication.
   */
  public static Record                        END_OF_DATA      = new Record( new RecordMetadata( new LinkedList<FieldMetadata>( ) ) );

  private RecordMetadata                      metadata;
  private List<Field<? extends Serializable>> data;

  /**
   * Creates a record that can store the kind of data described by the supplied metadata.
   * 
   * @param metadata the metadata describing the data this record may hold
   * @throws NullPointerException if the supplied metadata is <code>null</code>.
   */
  public Record( RecordMetadata metadata )
    {
    this( metadata, false );
    }

  /**
   * Creates a record that can store the kind of data described by the supplied metadata, possibly setting all the fields to the
   * <code>null</code> value.
   * 
   * @param metadata the metadata describing the data this record may hold
   * @param nullFields <code>true</code> to indicate that all fields should be set to <code>null</code>.
   * @throws NullPointerException if the supplied metadata is <code>null</code>.
   */
  public Record( RecordMetadata metadata, boolean nullFields )
    {
    this.metadata = new RecordMetadata( metadata.getFieldMetadata( ) );
    this.data = new ArrayList<Field<? extends Serializable>>( Collections.nCopies( this.metadata.getFieldCount( ),
        (Field<Serializable>) null ) );
    if( nullFields )
      {
      this.nullFields( this.getMetadata( ).getFieldNames( ) );
      }
    }

  /**
   * Returns the metadata associated with this record. The instance returned is a copy of the real metadata, so if you want to change
   * the metadata associated with this record, use one of the supplied mutator methods ({@link Record#remove(Collection)},
   * {@link Record#retain(Collection)} or {@link Record#add(Record)}).
   * 
   * @return the metadata associated with this record.
   */
  public RecordMetadata getMetadata( )
    {
    return this.metadata.clone( );
    }

  /**
   * Returns the field with the supplied name. More specifically, the field name is searched in the list of field metadata, and then
   * the field in the corresponding position is retrieved and returned. If no field metadata is found with the given name this method
   * throws an exception.
   * 
   * @param fieldName the name of the field to retrieve.
   * @return the field with the supplied name.
   * @throws NoSuchFieldNameException if no field metadata can be found with the specified name.
   */
  public Field<? extends Serializable> getField( String fieldName )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    if( fieldIndex != -1 )
      {
      return this.data.get( fieldIndex );
      }
    else
      {
      throw new NoSuchFieldNameException( "No field named " + fieldName );
      }
    }

  /**
   * <p>
   * Sets the field with the specified name to the value provided. If no field metadata is found with the given name this method throws
   * an exception.
   * </p>
   * <p>
   * <b>WARNING:</b> This method doesn't currently check that the field provided holds a valid value taking into account the
   * corresponding field metadata. This should be fixed as soon as possible.
   * </p>
   * 
   * @param fieldName the name of the field to set.
   * @param field the new value of the field.
   * @throws NoSuchFieldNameException if no field metadata can be found with the specified name.
   */
  public void setField( String fieldName, Field<?> field )
    {
    // FIXME: Check the type of the value against the field metadata
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    if( fieldIndex != -1 )
      {
      this.data.set( fieldIndex, field );
      }
    else
      {
      throw new NoSuchFieldNameException( "No field named " + fieldName );
      }
    }

  /**
   * Sets the fields of this record from the values contained in the provided record. The operation is done based on field names, so
   * fields of the same name are copied from the provided record to this record. Fields with names not included in this record are
   * ignored. The field order of this record is always preserved.
   * 
   * @param record the record which fields are copied to this record
   * @throws NullPointerException if the supplied record is <code>null</code>.
   */
  public void setFields( Record record )
    {
    for( FieldMetadata currentFieldMetadata : record.getMetadata( ).getFieldMetadata( ) )
      {
      String fieldName = currentFieldMetadata.getName( );
      if( this.metadata.getFieldIndex( fieldName ) != -1 )
        {
        this.setField( fieldName, record.getField( fieldName ) );
        }
      }
    }

  /**
   * Sets all the fields with names contained in the provided collection to the <code>null</code> value. This means that the
   * corresponding holes in the record will be filled with a {@link Field} instance containing the <code>null</code> value. Fields
   * with names not included in this record are ignored.
   * 
   * @param fieldNames the name of the field to be set to the <code>null</code> value.
   * @throws NullPointerException if the supplied collection is <code>null</code>.
   */
  public void nullFields( Collection<String> fieldNames )
    {
    for( String fieldName : fieldNames )
      {
      if( this.metadata.getFieldIndex( fieldName ) != -1 )
        {
        this.setValue( fieldName, null );
        }
      }
    }

  /**
   * Returns the value held by the field with the supplied name. This is just a convenience method to save us from an extra method call
   * and cast.
   * 
   * @param <T> the desired type of the returned value.
   * @param clazz the desired type of the returned value.
   * @param fieldName the name of the field which value will be retrieved.
   * @return the value held by the field with the supplied name.
   * @throws NoSuchFieldNameException if no field metadata can be found with the specified name.
   * @throws ClassCastException if the type provided is not compatible with the real type of the requested value.
   */
  public <T extends Serializable> T getValue( Class<T> clazz, String fieldName )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    if( fieldIndex != -1 )
      {
      Field<? extends Object> field = this.data.get( fieldIndex );
      return (T) field.getValue( );
      }
    else
      {
      throw new NoSuchFieldNameException( "No field named " + fieldName );
      }
    }

  /**
   * <p>
   * Sets the field with the specified name to the value provided. If no field metadata is found with the given name this method throws
   * an exception.
   * </p>
   * <p>
   * <b>WARNING:</b> This method doesn't currently check that the value provided is a valid value taking into account the
   * corresponding field metadata. This should be fixed as soon as possible.
   * </p>
   * 
   * @param <T> the type of the value to set.
   * @param fieldName the name of the field to set.
   * @param value the new value of the field.
   * @throws NoSuchFieldNameException if no field metadata can be found with the specified name.
   */
  public <T extends Serializable> void setValue( String fieldName, T value )
    {
    int fieldIndex = this.metadata.getFieldIndex( fieldName );
    if( fieldIndex != -1 )
      {
      this.data.set( fieldIndex, new Field<T>( value ) );
      }
    else
      {
      throw new NoSuchFieldNameException( "No field named " + fieldName );
      }
    }

  /**
   * Removes from this record all the field metadata and all the fields with names included in the supplied collection.
   * 
   * @param fieldNames the names of the field metadata and fields to remove.
   * @throws NullPointerException if the specified collection is <code>null</code>.
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
   * Retains all the field metadata and fields with names included in the suppled collection. In other words, removes from this record
   * all the field metadata and fields with names not included in the supplied collection.
   * 
   * @param fieldNames the names of the field metadata and fields to keep.
   * @throws NullPointerException if the specified collection is <code>null</code>.
   */
  public void retain( Collection<String> fieldNames )
    {
    List<String> fieldsToRemove = new LinkedList<String>( this.metadata.getFieldNames( ) );
    fieldsToRemove.removeAll( fieldNames );
    this.remove( fieldsToRemove );
    }

  /**
   * Appends the field metadata and fields contained in the supplied record to the end of the field metadata and fields contained in
   * this record.
   * 
   * @param record the record which field metadata and fields will be appended to this record.
   * @throws NullPointerException if <code>record</code> is <code>null</code>.
   * @throws IllegalArgumentException if the the supplied record contains some field metadata with the same name that some field
   *           metadata in this record.
   */
  public void add( Record record )
    {
    this.metadata.add( record.getMetadata( ) );
    this.data.addAll( record.data );
    }

  /**
   * Returns a record containing all the field metadata and fields of this record with names not included in the supplied collection.
   * 
   * @param fieldNames the names of the field metadata and fields to remove.
   * @return a record with the same field metadata and fields that this record, supressing the specified field metadata and fields.
   * @throws NullPointerException if the specified collection is <code>null</code>.
   */
  public Record supress( Collection<String> fieldNames )
    {
    List<String> fieldNamesToExtract = new LinkedList<String>( this.metadata.getFieldNames( ) );
    fieldNamesToExtract.removeAll( fieldNames );
    return this.extract( fieldNamesToExtract );
    }

  /**
   * Returns a record containing all the field metadata and fields of this record with names included in the supplied list. The order
   * given in the supplied list is preserved in the resulting record.
   * 
   * @param fieldNames the names of the field metadata and fields to extract.
   * @return a record containing all the field metadata and fields in this record which field name is included in the supplied list.
   * @throws NullPointerException if the specified collection is <code>null</code>.
   * @throws IllegalArgumentException if the supplied list contains duplicated names.
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
   * Returns a record containing the field metadata and fields of this record concatenated with the field metadata and fields contained
   * in the supplied record.
   * 
   * @param record the record which field metadata and fields will be appended to this record.
   * @return a record containing the concatenated field metadata and fields of this record and the supplied record.
   * @throws NullPointerException if <code>record</code> is <code>null</code>.
   * @throws IllegalArgumentException if the the supplied record contains some field metadata with the same name that some field
   *           metadata in this record.
   */
  public Record concatenate( Record record )
    {
    Record newRecord = new Record( this.getMetadata( ).concatenate( record.getMetadata( ) ) );
    newRecord.data = (List<Field<? extends Serializable>>) ((ArrayList<Field<? extends Serializable>>) this.data).clone( );
    newRecord.data.addAll( record.data );
    return newRecord;
    }

  /**
   * Compares the specified object with this record for equality. Returns <code>true</code> if and only if the specified object is
   * also a record, and the list of field metadata and fields are equal.
   * 
   * @param object The object to be compared for equality with this record.
   * @return <code>true</code> if the specified object is equal to this record.
   */
  @Override
  public boolean equals( Object object )
    {
    return object instanceof Record && this.metadata.equals( ((Record) object).metadata ) && this.data.equals( ((Record) object).data );
    }

  /**
   * Returns the hash code value for this record.
   * 
   * @return the hash code value for this record.
   */
  @Override
  public int hashCode( )
    {
    return this.metadata.hashCode( ) ^ this.data.hashCode( );
    }

  /**
   * Returns a copy of this record. A shallow copy of the private instance variables is done, so changes in the cloned record doesn't
   * affect the original instance.
   * 
   * @return a clone of this <code>RecordMetadata</code> instance
   */
  @Override
  public Record clone( )
    {
    try
      {
      Record clonedRecord = (Record) super.clone( );
      clonedRecord.metadata = (RecordMetadata) this.metadata.clone( );
      clonedRecord.data = (List<Field<? extends Serializable>>) ((ArrayList<Field<? extends Serializable>>) this.data).clone( );
      return clonedRecord;
      }
    catch( CloneNotSupportedException exc )
      {
      exc.printStackTrace( );
      throw new InternalError( );
      }
    }

  /**
   * Returns a string representation of this record. The string representation is a comma separated list of values surrounded by square
   * brackets.
   * 
   * @return a string representation of this record.
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

  /**
   * Compares this record with the specified record for order. The order is determined comparing the records field by field preserving
   * the order specified by the metadata. In order to compare two records, they must have the same metadata, if they don't a
   * <code>ClassCastException</code> is thrown. This exception is also thrown if some of the values in this record doesn't implement
   * <code>Comparable</code>.
   * 
   * @param record the record to be compared.
   * @return a negative integer, zero, or a positive integer as this record is less than, equal to, or greater than the specified
   *         record.
   * @throws ClassCastException if the metadata aren't equal, so it's impossible to compare the records.
   * @throws ClassCastException if some of the values in this record doesn't implement <code>Comparable</code>.
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
