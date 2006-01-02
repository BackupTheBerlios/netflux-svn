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
 * Thrown to indicate that a method has been passed a field name that can't be found.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class NoSuchFieldNameException extends IllegalArgumentException
  {
  private static final long serialVersionUID = -3325590738881884332L;

  /**
   * Constructs a <code>NoSuchFieldNameException</code> with no detail message.
   */
  public NoSuchFieldNameException( )
    {
    super( );
    }

  /**
   * Constructs a <code>NoSuchFieldNameException</code> with the specified detail message.
   * 
   * @param message the detail message.
   */
  public NoSuchFieldNameException( String message )
    {
    super( message );
    }
  }
