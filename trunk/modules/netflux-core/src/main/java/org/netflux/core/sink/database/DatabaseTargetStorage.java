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
package org.netflux.core.sink.database;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

import javax.sql.DataSource;

import org.netflux.core.Field;
import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.sink.TargetDataStorage;
import org.netflux.core.sink.TargetDataStorageException;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class DatabaseTargetStorage implements TargetDataStorage
  {
  private DataSource                dataSource;
  private Connection                connection;
  private BindablePreparedStatement updateStatement;
  private String                    sql;

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
   * @return Returns the sql.
   */
  public String getSql( )
    {
    return this.sql;
    }

  /**
   * @param sql The sql to set.
   */
  public void setSql( String sql )
    {
    this.sql = sql;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.sink.TargetDataStorage#storeRecord(org.netflux.core.Record)
   */
  public void storeRecord( Record record ) throws TargetDataStorageException
    {
    try
      {
      if( this.connection == null )
        {
        this.connection = this.getDataSource( ).getConnection( );
        this.connection.setAutoCommit( false );
        this.updateStatement = BindablePreparedStatement.createBindablePreparedStatement( this.connection, this.getSql( ) );
        }

      for( FieldMetadata fieldMetadata : record.getMetadata( ).getFieldMetadata( ) )
        {
        if( this.updateStatement.containsParameter( fieldMetadata.getName( ) ) )
          {
          this.populateDatabaseField( record, fieldMetadata );
          }
        }
      this.updateStatement.executeUpdate( );
      this.connection.commit( );
      }
    catch( SQLException exc )
      {
      throw new TargetDataStorageException( "SQL exception while trying to store record in database.", true, exc );
      }
    }

  /**
   * @param record
   * @param fieldMetadata
   * @throws TargetDataStorageException
   */
  protected void populateDatabaseField( Record record, FieldMetadata fieldMetadata ) throws TargetDataStorageException
    {
    try
      {
      String fieldName = fieldMetadata.getName( );
      Field<? extends Object> field = record.getField( fieldName );

      if( field.getValue( ) != null )
        {
        switch( fieldMetadata.getType( ) )
          {
          case Types.CHAR:
          case Types.VARCHAR:
            this.updateStatement.setString( fieldName, record.getValue( String.class, fieldName ) );
            break;

          case Types.DATE:
          case Types.TIMESTAMP:
            this.updateStatement.setDate( fieldName, new java.sql.Date( record.getValue( Date.class, fieldName ).getTime( ) ) );
            break;

          case Types.SMALLINT:
          case Types.INTEGER:
            this.updateStatement.setInt( fieldName, record.getValue( Integer.class, fieldName ) );
            break;

          case Types.BIGINT:
            this.updateStatement.setBigDecimal( fieldName, record.getValue( BigDecimal.class, fieldName ) );
            break;

          case Types.DECIMAL:
            this.updateStatement.setBigDecimal( fieldName, record.getValue( BigDecimal.class, fieldName ) );
            break;

          case Types.FLOAT:
            this.updateStatement.setFloat( fieldName, record.getValue( Float.class, fieldName ) );
            break;

          case Types.DOUBLE:
            this.updateStatement.setDouble( fieldName, record.getValue( Double.class, fieldName ) );
            break;

          case Types.BOOLEAN:
            this.updateStatement.setBoolean( fieldName, record.getValue( Boolean.class, fieldName ) );
            break;

          default:
            throw new TargetDataStorageException( "Type not supported: " + fieldMetadata.getType( ) + " while trying to store: "
                + fieldMetadata.getName( ) );
          }
        }
      else
        {
        this.updateStatement.setNull( fieldName, fieldMetadata.getType( ) );
        }
      }
    catch( SQLException exc )
      {
      throw new TargetDataStorageException( "SQL exception while trying to store: " + fieldMetadata.getName( ), exc );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.sink.TargetDataStorage#close()
   */
  public void close( )
    {
    if( this.connection != null )
      {
      try
        {
        this.updateStatement.close( );
        }
      catch( SQLException e )
        {}
      try
        {
        this.connection.close( );
        }
      catch( SQLException e )
        {}
      }
    }
  }
