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
package org.netflux.core.sink;

// TODO: Rethink implementation of fatal errors while writing in underlying storage. I feel a more object oriented approach would
// map a fatal error to another exception class, instead of having a fatal attribute. This would be more friendly with the Java
// exception handling mechanism
/**
 * Thrown to indicate a problem in the underlying data storage.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class TargetDataStorageException extends Exception
  {
  private static final long serialVersionUID = 5472811871346079061L;

  private boolean           fatal;

  /**
   * @param message
   */
  public TargetDataStorageException( String message )
    {
    this( message, false );
    }

  /**
   * @param message
   * @param throwable
   */
  public TargetDataStorageException( String message, Throwable throwable )
    {
    this( message, false, throwable );
    }

  /**
   * @param message
   * @param fatal
   */
  public TargetDataStorageException( String message, boolean fatal )
    {
    super( message );
    this.fatal = fatal;
    }

  /**
   * @param message
   * @param fatal
   * @param throwable
   */
  public TargetDataStorageException( String message, boolean fatal, Throwable throwable )
    {
    super( message, throwable );
    this.fatal = fatal;
    }

  /**
   * @return Returns the fatal.
   */
  public boolean isFatal( )
    {
    return this.fatal;
    }
  }
