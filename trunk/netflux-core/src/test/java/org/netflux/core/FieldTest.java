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

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * Tests for {@link Field} class.
 * 
 * @author Last modified by $Author$
 */
public class FieldTest extends TestCase
  {
  private Field<String> fieldNull;
  private Field<String> fieldA;
  private Field<String> fieldB;
  private Object        object;

  /*
   * @see TestCase#setUp()
   */
  protected void setUp( ) throws Exception
    {
    this.fieldNull = new Field<String>( null );
    this.fieldA = new Field<String>( "A" );
    this.fieldB = new Field<String>( "B" );
    this.object = new Integer( 3 );
    }

  /**
   * Tests correct operation of <code>equals</code> method.
   */
  public void testFieldEquals( )
    {
    // Test for equality
    Assert.assertEquals( fieldNull, new Field<String>( null ) );
    Assert.assertEquals( fieldA, new Field<String>( "A" ) );
    Assert.assertEquals( fieldB, new Field<String>( "B" ) );

    // Test for handling null or objects that aren't a Field
    Assert.assertFalse( fieldNull.equals( null ) );
    Assert.assertFalse( fieldNull.equals( this.object ) );
    Assert.assertFalse( fieldA.equals( null ) );
    Assert.assertFalse( fieldA.equals( this.object ) );

    // Some equality tests that should always yield false
    Assert.assertFalse( fieldNull.equals( fieldA ) );
    Assert.assertFalse( fieldA.equals( fieldB ) );
    }

  /**
   * Tests correct operation of <code>hashCode</code> method.
   */
  public void testFieldHashCode( )
    {
    Assert.assertEquals( fieldNull.hashCode( ), 0 );
    Assert.assertTrue( fieldA.equals( new Field<String>( "A" ) ) ? fieldA.hashCode( ) == new Field<String>( "A" ).hashCode( ) : true );
    Assert.assertTrue( fieldA.equals( fieldB ) ? fieldA.hashCode( ) == fieldB.hashCode( ) : true );
    }
  }
