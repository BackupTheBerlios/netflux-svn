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

/**
 * A container for a piece of data. A field just contains a reference to a value and provides convenience methods for hash code,
 * equality and conversion to {@link java.lang.String} taking into account <code>null</code> values. A field is inmutable, this is,
 * once created there is no possibility of changing the associated value.
 * 
 * @param <T> Type of the value to hold.
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class Field<T extends Serializable> implements Serializable
  {
  private static final long serialVersionUID = 2062257821266476335L;

  private T                 value;

  /**
   * Creates a <code>Field</code> holding the specified <code>value</code>.
   * 
   * @param value The <code>value</code> to be held by this <code>Field</code>.
   */
  public Field( T value )
    {
    this.value = value;
    }

  /**
   * Returns the value held by this field.
   * 
   * @return the value held by this field.
   */
  public T getValue( )
    {
    return this.value;
    }

  /**
   * Compares the specified object with this field for equality. Returns <code>true</code> if and only if the specified object is
   * also a field, and the values held by them are equal. Values are considered to be equal if they are both <code>null</code> or if
   * they are both different from <code>null</code> and the call to <code>equals</code> returns <code>true</code>.
   * 
   * @param object The object to be compared for equality with this field.
   * @return <code>true</code> if the specified object is equal to this field.
   */
  @Override
  public boolean equals( Object object )
    {
    if( object instanceof Field )
      {
      Field field = (Field) object;
      return this == field || ((this.value == null) ? field.value == null : this.value.equals( field.value ));
      }
    else
      {
      return false;
      }
    }

  /**
   * Returns the hash code value for this field. The hash code is computed using the hash code method of the held value. If the value
   * is <code>null</code> this method returns <code>0</code>.
   * 
   * @return the hash code value for this field.
   */
  @Override
  public int hashCode( )
    {
    return (this.value != null) ? this.value.hashCode( ) : 0;
    }

  /**
   * Returns a string representation of this field. The string representation is created calling the <code>toString</code> method on
   * the held value. If the value is <code>null</code> then this method returns a string containing {@code <NULL>}.
   * 
   * @return a string representation of this field.
   */
  @Override
  public String toString( )
    {
    return (this.value != null) ? this.value.toString( ) : "<NULL>";
    }
  }
