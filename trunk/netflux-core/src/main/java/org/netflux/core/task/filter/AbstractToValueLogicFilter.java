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
package org.netflux.core.task.filter;

/**
 * Abstract base class for logic filters with operations between a field and a value.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractToValueLogicFilter extends AbstractLogicFilter
  {
  private Object value;

  /**
   * Returns the value to be used in the logic operation.
   * 
   * @return the value to be used in the logic operation.
   */
  public Object getValue( )
    {
    return value;
    }

  /**
   * Sets the value to be used in the logic operation.
   * 
   * @param fieldName the value to be used in the logic operation.
   */
  public void setValue( Object value )
    {
    this.value = value;
    }
  }
