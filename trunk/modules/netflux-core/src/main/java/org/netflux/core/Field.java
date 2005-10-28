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

/**
 * @author jgonzalez
 */
public class Field<T>
  {
  private T value;

  /**
   * @param value
   */
  public Field( T value )
    {
    this.value = value;
    }

  /**
   * @return Returns the value.
   */
  public T getValue( )
    {
    return this.value;
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals( Object object )
    {
    if( object instanceof Field )
      {
      Field field = (Field) object;
      return (this.value == null) ? field.value == null : this.value.equals( field.value );
      }
    else
      {
      return false;
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode( )
    {
    return (this.value != null) ? this.value.hashCode( ) : 0;
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString( )
    {
    return (this.value != null) ? this.value.toString( ) : "<NULL>";
    }
  }
