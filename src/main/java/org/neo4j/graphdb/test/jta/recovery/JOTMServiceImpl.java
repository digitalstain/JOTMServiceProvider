/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.graphdb.test.jta.recovery;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import javax.naming.NamingException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.objectweb.jotm.Current;
import org.objectweb.jotm.Jotm;
import org.objectweb.jotm.TransactionResourceManager;
import org.objectweb.transaction.jta.TransactionManager;

/**
 * This class is an implementation of a provider for Transaction Managers for
 * Neo. It uses the JOTM XA compatible implementation from ow2 to do the actual
 * work.
 *
 * At its present form it is more of a proof of concept rather than a mission
 * ready solution. It uses a local registry to store a local implementation.
 * Normally this should be changed to a standalone JOTM instance or a JNDI
 * retrieved one.
 *
 * @author Chris Gioran
 */
class JOTMServiceImpl extends AbstractTransactionManager
{
    private final TransactionManager current;
    private final Jotm jotm;

    JOTMServiceImpl()
    {
        Registry registry = null;
        try
        {
            registry = LocateRegistry.getRegistry( 1099 );
        }
        catch ( RemoteException re )
        {
            // Nothing yet, we can still create it.
        }
        if ( registry == null )
        {
            try
            {
                registry = LocateRegistry.createRegistry( 1099 );
            }
            catch ( RemoteException re )
            {
                // Something is fishy here, plus it is impossible to continue.
                // So we die.
                throw new Error( re );
            }
        }
        try
        {
            jotm = new Jotm( true, false );
            current = jotm.getTransactionManager();
        }
        catch ( NamingException ne )
        {
            throw new Error( "Error during JOTM creation", ne );
        }
    }

    /**
     * Starts the registry and binds a JOTM instance to it. Registers the
     * resource adapters declared by the neo data source manager to get ready
     * for possible recovery.
     */
    @Override
    public void init( XaDataSourceManager xaDsManager )
    {
        TransactionResourceManager trm = new TransactionResourceManager()
        {
            public void returnXAResource( String rmName, XAResource rmXares )
            {
                return;
            }
        };
        try
        {
            for ( XaDataSource xaDs : xaDsManager.getAllRegisteredDataSources() )
            {
                System.out.println("Registering another xa resource named: "+xaDs.getName());
                Current.getTransactionRecovery().registerResourceManager( xaDs.getName(),
                        xaDs.getXaConnection().getXaResource(), xaDs.getName(), trm );
            }
            Current.getTransactionRecovery().startResourceManagerRecovery();
        }
        catch ( XAException e )
        {
            throw new Error( "Error registering xa datasource", e );
        }
    }

    public void begin() throws NotSupportedException, SystemException
    {
        current.begin();
    }

    public void commit() throws RollbackException, HeuristicMixedException,
            HeuristicRollbackException, SecurityException,
            IllegalStateException, SystemException
    {
        current.commit();
    }

    public int getStatus() throws SystemException
    {
        return current.getStatus();
    }

    public Transaction getTransaction() throws SystemException
    {
        if ( current == null ) return null;
        return current.getTransaction();
    }

    public void resume( Transaction arg0 ) throws InvalidTransactionException,
            IllegalStateException, SystemException
    {
        current.resume( arg0 );
    }

    public void rollback() throws IllegalStateException, SecurityException,
            SystemException
    {
        current.rollback();
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        current.setRollbackOnly();
    }

    public void setTransactionTimeout( int arg0 ) throws SystemException
    {
        current.setTransactionTimeout( arg0 );
    }

    public Transaction suspend() throws SystemException
    {
        return current.suspend();
    }

    /**
     * Stops the JOTM instance.
     */
    @Override
    public void stop()
    {
        jotm.stop();
    }
}
