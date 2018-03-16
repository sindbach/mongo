/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/db_raii.h"

#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/curop.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/session_catalog.h"

namespace mongo {
namespace {

const boost::optional<int> kDoNotChangeProfilingLevel = boost::none;

}  // namespace

AutoStatsTracker::AutoStatsTracker(OperationContext* opCtx,
                                   const NamespaceString& nss,
                                   Top::LockType lockType,
                                   boost::optional<int> dbProfilingLevel,
                                   Date_t deadline)
    : _opCtx(opCtx), _lockType(lockType) {
    if (!dbProfilingLevel) {
        // No profiling level was determined, attempt to read the profiling level from the Database
        // object.
        AutoGetDb autoDb(_opCtx, nss.db(), MODE_IS, deadline);
        if (autoDb.getDb()) {
            dbProfilingLevel = autoDb.getDb()->getProfilingLevel();
        }
    }

    stdx::lock_guard<Client> clientLock(*_opCtx->getClient());
    CurOp::get(_opCtx)->enter_inlock(nss.ns().c_str(), dbProfilingLevel);
}

AutoStatsTracker::~AutoStatsTracker() {
    auto curOp = CurOp::get(_opCtx);
    Top::get(_opCtx->getServiceContext())
        .record(_opCtx,
                curOp->getNS(),
                curOp->getLogicalOp(),
                _lockType,
                durationCount<Microseconds>(curOp->elapsedTimeExcludingPauses()),
                curOp->isCommand(),
                curOp->getReadWriteType());
}

AutoGetCollectionForRead::AutoGetCollectionForRead(OperationContext* opCtx,
                                                   const NamespaceStringOrUUID& nsOrUUID,
                                                   AutoGetCollection::ViewMode viewMode,
                                                   Date_t deadline) {
    const auto collectionLockMode = getLockModeForQuery(opCtx);
    _autoColl.emplace(opCtx, nsOrUUID, collectionLockMode, viewMode, deadline);

    while (true) {
        auto coll = _autoColl->getCollection();
        if (!coll) {
            return;
        }

        auto minSnapshot = coll->getMinimumVisibleSnapshot();
        if (!minSnapshot) {
            return;
        }
        auto mySnapshot = opCtx->recoveryUnit()->getPointInTimeReadTimestamp();
        if (!mySnapshot) {
            return;
        }
        if (mySnapshot >= minSnapshot) {
            return;
        }

        auto readConcernLevel = opCtx->recoveryUnit()->getReadConcernLevel();
        if (readConcernLevel == repl::ReadConcernLevel::kSnapshotReadConcern) {
            uasserted(ErrorCodes::SnapshotUnavailable,
                      str::stream()
                          << "Unable to read from a snapshot due to pending collection catalog "
                             "changes; please retry the operation. Snapshot timestamp is "
                          << mySnapshot->toString()
                          << ". Collection minimum is "
                          << minSnapshot->toString());
        }
        invariant(readConcernLevel == repl::ReadConcernLevel::kMajorityReadConcern);

        // Yield locks in order to do the blocking call below
        _autoColl = boost::none;

        repl::ReplicationCoordinator::get(opCtx)->waitUntilSnapshotCommitted(opCtx, *minSnapshot);

        uassertStatusOK(opCtx->recoveryUnit()->obtainMajorityCommittedSnapshot());

        {
            stdx::lock_guard<Client> lk(*opCtx->getClient());
            CurOp::get(opCtx)->yielded();
        }

        _autoColl.emplace(opCtx, nsOrUUID, collectionLockMode, viewMode, deadline);
    }
}

AutoGetCollectionForReadCommand::AutoGetCollectionForReadCommand(
    OperationContext* opCtx,
    const NamespaceStringOrUUID& nsOrUUID,
    AutoGetCollection::ViewMode viewMode,
    Date_t deadline)
    : _autoCollForRead(opCtx, nsOrUUID, viewMode, deadline),
      _statsTracker(opCtx,
                    _autoCollForRead.getNss(),
                    Top::LockType::ReadLocked,
                    _autoCollForRead.getDb() ? _autoCollForRead.getDb()->getProfilingLevel()
                                             : kDoNotChangeProfilingLevel,
                    deadline) {
    if (!_autoCollForRead.getView()) {
        // We have both the DB and collection locked, which is the prerequisite to do a stable shard
        // version check, but we'd like to do the check after we have a satisfactory snapshot.
        auto css = CollectionShardingState::get(opCtx, _autoCollForRead.getNss());
        css->checkShardVersionOrThrow(opCtx);
    }
}

OldClientContext::OldClientContext(OperationContext* opCtx, const std::string& ns, bool doVersion)
    : OldClientContext(opCtx, ns, doVersion, dbHolder().get(opCtx, ns), false) {}

OldClientContext::OldClientContext(
    OperationContext* opCtx, const std::string& ns, bool doVersion, Database* db, bool justCreated)
    : _opCtx(opCtx), _db(db), _justCreated(justCreated) {
    if (!_db) {
        const auto dbName = nsToDatabaseSubstring(ns);
        invariant(_opCtx->lockState()->isDbLockedForMode(dbName, MODE_X));
        _db = dbHolder().openDb(_opCtx, dbName, &_justCreated);
        invariant(_db);
    }

    auto const currentOp = CurOp::get(_opCtx);

    if (doVersion) {
        switch (currentOp->getNetworkOp()) {
            case dbGetMore:  // getMore is special and should be handled elsewhere
            case dbUpdate:   // update & delete check shard version as part of the write executor
            case dbDelete:   // path, so no need to check them here as well
                break;
            default:
                auto css = CollectionShardingState::get(_opCtx, ns);
                css->checkShardVersionOrThrow(_opCtx);
                break;
        }
    }

    stdx::lock_guard<Client> lk(*_opCtx->getClient());
    currentOp->enter_inlock(ns.c_str(), _db->getProfilingLevel());
}

OldClientContext::~OldClientContext() {
    // If in an interrupt, don't record any stats.
    // It is possible to have no lock after saving the lock state and being interrupted while
    // waiting to restore.
    if (_opCtx->getKillStatus() != ErrorCodes::OK)
        return;

    invariant(_opCtx->lockState()->isLocked());
    auto currentOp = CurOp::get(_opCtx);
    Top::get(_opCtx->getClient()->getServiceContext())
        .record(_opCtx,
                currentOp->getNS(),
                currentOp->getLogicalOp(),
                _opCtx->lockState()->isWriteLocked() ? Top::LockType::WriteLocked
                                                     : Top::LockType::ReadLocked,
                _timer.micros(),
                currentOp->isCommand(),
                currentOp->getReadWriteType());
}


OldClientWriteContext::OldClientWriteContext(OperationContext* opCtx, StringData ns)
    : _opCtx(opCtx), _nss(ns) {
    // Lock the database and collection
    _autoCreateDb.emplace(opCtx, _nss.db(), MODE_IX);
    _collLock.emplace(opCtx->lockState(), _nss.ns(), MODE_IX);

    // TODO (Kal): None of the places which use OldClientWriteContext seem to require versioning, so
    // we should consider defaulting this to false
    const bool doShardVersionCheck = true;
    _clientContext.emplace(opCtx,
                           _nss.ns(),
                           doShardVersionCheck,
                           _autoCreateDb->getDb(),
                           _autoCreateDb->justCreated());
    invariant(_autoCreateDb->getDb() == _clientContext->db());

    // If the collection exists, there is no need to lock into stronger mode
    if (getCollection())
        return;

    // If the database was just created, it is already locked in MODE_X so we can skip the relocking
    // code below
    if (_autoCreateDb->justCreated()) {
        dassert(opCtx->lockState()->isDbLockedForMode(_nss.db(), MODE_X));
        return;
    }

    // If the collection doesn't exists, put the context in a state where the database is locked in
    // MODE_X so that the collection can be created
    _clientContext.reset();
    _collLock.reset();
    _autoCreateDb.reset();
    _autoCreateDb.emplace(opCtx, _nss.db(), MODE_X);

    _clientContext.emplace(opCtx,
                           _nss.ns(),
                           doShardVersionCheck,
                           _autoCreateDb->getDb(),
                           _autoCreateDb->justCreated());
    invariant(_autoCreateDb->getDb() == _clientContext->db());
}

LockMode getLockModeForQuery(OperationContext* opCtx) {
    invariant(opCtx);

    // Use IX locks for autocommit:false multi-statement transactions; otherwise, use IS locks.
    auto session = OperationContextSession::get(opCtx);
    if (session && session->inMultiDocumentTransaction()) {
        return MODE_IX;
    }
    return MODE_IS;
}

}  // namespace mongo
