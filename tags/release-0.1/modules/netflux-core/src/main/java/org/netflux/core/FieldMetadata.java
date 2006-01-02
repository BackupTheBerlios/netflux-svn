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
import java.sql.Types;

// TODO: Improve Javadoc documentation when precision, scale and nullable get really used in the framework
/**
 * A desription of the characteristics of a value that may be held in a {@link Field}. More formally, a <code>FieldMetadata</code>
 * object contains the following information:
 * <dl>
 * <dt>name</dt>
 * <dd>Name of the field</dd>
 * <dt>type</dt>
 * <dd>Type of the field, using one of the supported types specified by {@link java.sql.Types}. For a list of supported types see
 * {@link FieldMetadata#setType(int).</dd>
 * <dt>precision</dt>
 * <dd>Length of the field, or in the case of decimal fields, number of digits of the field</dd>
 * <dt>scale</dt>
 * <dd>Number of digits to the right of the decimal point, in case of decimal fields</dd>
 * <dt>nullable</dt>
 * <dd>Nullability of the field</dd>
 * </dl>
 * <b>NOTE:</b> Currently only name and type are thoroughly used in the framework, and there are no checks for incompatibilities in
 * size, scale or nullability of fields. This should be addressed in a near future.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class FieldMetadata implements Serializable, Cloneable
  {
  private static final long serialVersionUID = 3920443747044944958L;

  private String            name             = "undefined";
  private int               type             = Types.VARCHAR;
  private int               precision        = 256;
  private int               scale            = 0;
  private boolean           nullable         = true;

  /**
   * Creates a default <code>FieldMetadata</code>. The created object will have the literal <code>undefined</code> as
   * <code>name</code>, a <code>VARCHAR</code> type, a <code>precision</code> of <code>256</code>, a <code>scale</code> of
   * <code>0</code> and will be nullable.
   */
  public FieldMetadata( )
    {}

  /**
   * Creates a <code>FieldMetadata</code> object with the supplied <code>name</code> and <code>type</code>. The created object
   * will have a <code>precision</code> of <code>256</code>, a <code>scale</code> of <code>0</code> and will be nullable.
   * 
   * @param name the name of the field that this metadata describes
   * @param type the type of the field that this metadata describes
   * @throws NullPointerException if the specified <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified <code>name</code> or <code>type</code> doesn't contain valid values (See
   *           {@link FieldMetadata#setName(String)} and {@link FieldMetadata#setType(int)} for more information).
   */
  public FieldMetadata( String name, int type )
    {
    this.setName( name );
    this.setType( type );
    }

  /**
   * Returns the name of the field that this metadata describes.
   * 
   * @return the name of the field that this metadata describes.
   */
  public String getName( )
    {
    return this.name;
    }

  /**
   * Sets the <code>name</code> of the field that this metadata describes. Null values, empty strings and strings only containing
   * spaces are forbidden, and an exception will be thrown in such cases.
   * 
   * @param name the name of the field that this metadata describes
   * @throws NullPointerException if the specified <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified <code>name</code> is the empty string, or only contains spaces
   */
  public void setName( String name )
    {
    if( name == null )
      {
      throw new NullPointerException( );
      }
    else if( name.trim( ).equals( "" ) )
      {
      throw new IllegalArgumentException( );
      }
    else
      {
      this.name = name;
      }
    }

  /**
   * Returns the type of the field that this metadata describes.
   * 
   * @return the type of the field that this metadata describes.
   */
  public int getType( )
    {
    return this.type;
    }

  /**
   * Sets the <code>type</code> of the field that this metadata describes. The currently supported types are: string ({@link java.sql.Types#CHAR},
   * {@link java.sql.Types#VARCHAR}), date ({@link java.sql.Types#DATE}, {@link java.sql.Types#TIMESTAMP}), numeric ({@link java.sql.Types#SMALLINT},
   * {@link java.sql.Types#INTEGER}, {@link java.sql.Types#BIGINT}, {@link java.sql.Types#DECIMAL}, {@link java.sql.Types#FLOAT},
   * {@link java.sql.Types#DOUBLE}) and boolean ({@link java.sql.Types#BOOLEAN}). If the supplied <code>type</code> is not one of
   * the above, an <code>IllegalArgumentException</code> will be thrown.
   * 
   * @param type the <code>type</code> of the field that this metadata describes.
   * @throws IllegalArgumentException if the supplied <code>type</code> is not included in the supported types.
   */
  public void setType( int type )
    {
    switch( type )
      {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.DATE:
      case Types.TIMESTAMP:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.DECIMAL:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.BOOLEAN:
        this.type = type;
        break;
      default:
        throw new IllegalArgumentException( );
      }
    }

  /**
   * Returns the precision of the field that this metadata describes.
   * 
   * @return the precision of the field that this metadata describes.
   */
  public int getPrecision( )
    {
    return this.precision;
    }

  /**
   * Sets the precision of the field that this metadata describes. The precision must be a positive integer, if not an exception will
   * be thrown.
   * 
   * @param precision the precision of the field that this metadata describes.
   * @throws IllegalArgumentException if the supplied <code>precision</code> is negative.
   */
  public void setPrecision( int precision )
    {
    if( precision > 0 )
      {
      this.precision = precision;
      }
    else
      {
      throw new IllegalArgumentException( );
      }
    }

  /**
   * Returns the scale of the field that this metadata describes.
   * 
   * @return the scale of the field that this metadata describes.
   */
  public int getScale( )
    {
    return this.scale;
    }

  /**
   * Sets the scale of the field that this metadata describes. The scale must be a positive integer and less than or equal to the
   * current precision, if not an exception will be thrown.
   * 
   * @param scale the scale of the field that this metadata describes.
   * @throws IllegalArgumentException if the supplied <code>scale</code> is negative or greater than the <code>precision</code>.
   */
  public void setScale( int scale )
    {
    if( scale >= 0 && scale <= this.getPrecision( ) )
      {
      this.scale = scale;
      }
    else
      {
      throw new IllegalArgumentException( "Scale [" + scale + "] must be a value between 0 and precision [" + this.getPrecision( )
          + "]" );
      }
    }

  /**
   * Returns <code>true</code> if the field this metadata describes may be set to the scale of the field that this metadata
   * describes.
   * 
   * @return <code>true</code> if the field this metadata describes may be set to <code>null</code>, <code>false</code>
   *         otherwise.
   */
  public boolean isNullable( )
    {
    return this.nullable;
    }

  /**
   * Sets the nullability of the field this metadata describes.
   * 
   * @param nullable the nullability of the field this metadata describes
   */
  public void setNullable( boolean nullable )
    {
    this.nullable = nullable;
    }

  /**
   * <p>
   * Compares the specified object with this metadata for equality. Returns <code>true</code> if and only if the specified object is
   * also a field metadata, and the attributes of both metadata (name, type, precision, scale and nullability) are equal.
   * </p>
   * <p>
   * <b>Note:</b> This equality condition could be relaxed, considering two metadata objects are equal if they describe the same kind
   * of data, this is, if all their attributes are equal except for the name. This could be implemented in the equals method, or even
   * better, an alternative method could be provided for doing this kind of comparison. If the need arises such an implementation would
   * be added.
   * </p>
   * 
   * @param object the object to be compared for equality with this metadata.
   * @return <code>true</code> if the specified object is equal to this metadata.
   */
  @Override
  public boolean equals( Object object )
    {
    if( object instanceof FieldMetadata )
      {
      FieldMetadata fieldMetadata = (FieldMetadata) object;
      return (this.name == null) ? fieldMetadata.name == null : this.name.equals( fieldMetadata.name )
          && this.type == fieldMetadata.type && this.scale == fieldMetadata.scale && this.precision == fieldMetadata.precision
          && this.nullable == fieldMetadata.nullable;
      }
    else
      {
      return false;
      }
    }

  /**
   * Returns the hash code value for this metadata.
   * 
   * @return the hash code value for this metadata.
   */
  @Override
  public int hashCode( )
    {
    return ((this.name == null) ? 0 : this.name.hashCode( )) ^ this.type ^ this.scale ^ this.precision ^ ((this.nullable) ? 1 : 0);
    }

  /**
   * Returns a copy of this metadata.
   * 
   * @return a clone of this <code>FieldMetadata</code> instance
   */
  @Override
  public FieldMetadata clone( )
    {
    try
      {
      // No need to do anything else
      return (FieldMetadata) super.clone( );
      }
    catch( CloneNotSupportedException exc )
      {
      exc.printStackTrace( );
      throw new InternalError( );
      }
    }

  /**
   * Returns a string representation of this field metadata. The string representation is a comma separated list of values surrounded
   * by brackets.
   * 
   * @return a string representation of this record metadata.
   */
  @Override
  public String toString( )
    {
    StringBuffer metadataString = new StringBuffer( "{" );

    metadataString.append( this.getName( ) );
    metadataString.append( ',' );
    metadataString.append( this.getType( ) );
    metadataString.append( ',' );
    metadataString.append( this.getPrecision( ) );
    metadataString.append( ',' );
    metadataString.append( this.getScale( ) );
    if( this.isNullable( ) )
      {
      metadataString.append( ',' );
      metadataString.append( 'N' );
      }

    metadataString.append( '}' );
    return metadataString.toString( );
    }
  }
