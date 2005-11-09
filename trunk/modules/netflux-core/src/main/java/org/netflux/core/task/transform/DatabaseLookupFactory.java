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
package org.netflux.core.task.transform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

/**
 * @author jgonzalez
 */
public class DatabaseLookupFactory implements LookupTableFactory
  {
  private DataSource                              dataSource;
  private String                                  tableName;
  private String                                  keyColumnName;
  private int                                     keyType;
  private String                                  valueColumnName;
  private int                                     valueType;
  private Map<? extends Object, ? extends Object> lookupTable;

  /**
   * @return Returns the dataSource.
   */
  public DataSource getDataSource( )
    {
    return this.dataSource;
    }

  /**
   * @param dataSource The dataSource to set.
   */
  public void setDataSource( DataSource dataSource )
    {
    this.dataSource = dataSource;
    }

  /**
   * @return Returns the tableName.
   */
  public String getTableName( )
    {
    return this.tableName;
    }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName( String tableName )
    {
    this.tableName = tableName;
    }

  /**
   * @return Returns the keyColumnName.
   */
  public String getKeyColumnName( )
    {
    return this.keyColumnName;
    }

  /**
   * @param keyColumnName The keyColumnName to set.
   */
  public void setKeyColumnName( String keyColumnName )
    {
    this.keyColumnName = keyColumnName;
    }

  /**
   * @return Returns the keyType.
   */
  public int getKeyType( )
    {
    return this.keyType;
    }

  /**
   * @param keyType The keyType to set.
   */
  public void setKeyType( int keyType )
    {
    this.keyType = keyType;
    }

  /**
   * @return Returns the valueColumnName.
   */
  public String getValueColumnName( )
    {
    return this.valueColumnName;
    }

  /**
   * @param valueColumnName The valueColumnName to set.
   */
  public void setValueColumnName( String valueColumnName )
    {
    this.valueColumnName = valueColumnName;
    }

  /**
   * @return Returns the valueType.
   */
  public int getValueType( )
    {
    return this.valueType;
    }

  /**
   * @param valueType The valueType to set.
   */
  public void setValueType( int valueType )
    {
    this.valueType = valueType;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.LookupTableFactory#getLookupTable()
   */
  public Map<? extends Object, ? extends Object> getLookupTable( )
    {
    if( this.lookupTable == null )
      {
      Map<Object, Object> lookupTable = new HashMap<Object, Object>( );
      Connection connection = null;
      PreparedStatement lookupTableStatement = null;
      ResultSet dbLookupTable = null;
      try
        {
        connection = this.getDataSource( ).getConnection( );
        lookupTableStatement = connection.prepareStatement( "select \"" + this.getKeyColumnName( ) + "\", \""
            + this.getValueColumnName( ) + "\" from \"" + this.getTableName( ) + "\"" );
        dbLookupTable = lookupTableStatement.executeQuery( );

        while( dbLookupTable.next( ) )
          {
          Object key = DatabaseLookupFactory.extractObject( dbLookupTable, this.getKeyColumnName( ), this.getKeyType( ) );
          Object value = DatabaseLookupFactory.extractObject( dbLookupTable, this.getValueColumnName( ), this.getValueType( ) );
          lookupTable.put( key, value );
          }
        }
      catch( SQLException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      finally
        {
        if( dbLookupTable != null )
          {
          try
            {
            dbLookupTable.close( );
            }
          catch( SQLException exc )
            {}
          }
        if( lookupTableStatement != null )
          {
          try
            {
            lookupTableStatement.close( );
            }
          catch( SQLException exc )
            {}
          }
        if( connection != null )
          {
          try
            {
            connection.close( );
            }
          catch( SQLException exc )
            {}
          }
        }

      this.lookupTable = lookupTable;
      }

    return this.lookupTable;
    }

  private static Object extractObject( ResultSet resultSet, String columnName, int targetType ) throws SQLException
    {
    switch( targetType )
      {
      case Types.CHAR:
      case Types.VARCHAR:
        return resultSet.getString( columnName );

      case Types.DATE:
      case Types.TIMESTAMP:
        // We can't know this, so we just return if there is a string available
        return resultSet.getDate( columnName );

      case Types.SMALLINT:
      case Types.INTEGER:
        return resultSet.getInt( columnName );

      case Types.BIGINT:
      case Types.DECIMAL:
        return resultSet.getBigDecimal( columnName );

      case Types.FLOAT:
        return resultSet.getFloat( columnName );

      case Types.DOUBLE:
        return resultSet.getDouble( columnName );

      case Types.BOOLEAN:
        return resultSet.getBoolean( columnName );

      default:
        // An exception here!!!!!!!
        return null;
      }
    }
  }
