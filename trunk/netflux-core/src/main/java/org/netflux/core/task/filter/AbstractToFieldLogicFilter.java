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
 * Abstract base class for logic filters with operations between fields.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractToFieldLogicFilter extends AbstractLogicFilter
  {
  private String otherFieldName;

  /**
   * Returns the name of the second field to be used in the logic operation.
   * 
   * @return the name of the second field to be used in the logic operation.
   */
  public String getOtherFieldName( )
    {
    return otherFieldName;
    }

  /**
   * Sets the name of the second field to be used in the logic operation.
   * 
   * @param fieldName the name of the second field to be used in the logic operation.
   */
  public void setOtherFieldName( String otherFieldName )
    {
    this.otherFieldName = otherFieldName;
    }
  }
