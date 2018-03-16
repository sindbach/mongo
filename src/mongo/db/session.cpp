/**
 *    Copyright (C) 2017 MongoDB, Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/session.h"

#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/retryable_writes_stats.h"
#include "mongo/db/transaction_history_iterator.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

void fassertOnRepeatedExecution(const LogicalSessionId& lsid,
                                TxnNumber txnNumber,
                                StmtId stmtId,
                                const repl::OpTime& firstOpTime,
                                const repl::OpTime& secondOpTime) {
    severe() << "Statement id " << stmtId << " from transaction [ " << lsid.toBSON() << ":"
             << txnNumber << " ] was committed once with opTime " << firstOpTime
             << " and a second time with opTime " << secondOpTime
             << ". This indicates possible data corruption or server bug and the process will be "
                "terminated.";
    fassertFailed(40526);
}

struct ActiveTransactionHistory {
    boost::optional<SessionTxnRecord> lastTxnRecord;
    Session::CommittedStatementTimestampMap committedStatements;
    bool hasIncompleteHistory{false};
};

ActiveTransactionHistory fetchActiveTransactionHistory(OperationContext* opCtx,
                                                       const LogicalSessionId& lsid) {
    ActiveTransactionHistory result;

    result.lastTxnRecord = [&]() -> boost::optional<SessionTxnRecord> {
        DBDirectClient client(opCtx);
        auto result =
            client.findOne(NamespaceString::kSessionTransactionsTableNamespace.ns(),
                           {BSON(SessionTxnRecord::kSessionIdFieldName << lsid.toBSON())});
        if (result.isEmpty()) {
            return boost::none;
        }

        return SessionTxnRecord::parse(IDLParserErrorContext("parse latest txn record for session"),
                                       result);
    }();

    if (!result.lastTxnRecord) {
        return result;
    }

    auto it = TransactionHistoryIterator(result.lastTxnRecord->getLastWriteOpTime());
    while (it.hasNext()) {
        try {
            const auto entry = it.next(opCtx);
            invariant(entry.getStatementId());

            if (*entry.getStatementId() == kIncompleteHistoryStmtId) {
                // Only the dead end sentinel can have this id for oplog write history
                invariant(entry.getObject2());
                invariant(entry.getObject2()->woCompare(Session::kDeadEndSentinel) == 0);
                result.hasIncompleteHistory = true;
                continue;
            }

            const auto insertRes =
                result.committedStatements.emplace(*entry.getStatementId(), entry.getOpTime());
            if (!insertRes.second) {
                const auto& existingOpTime = insertRes.first->second;
                fassertOnRepeatedExecution(lsid,
                                           result.lastTxnRecord->getTxnNum(),
                                           *entry.getStatementId(),
                                           existingOpTime,
                                           entry.getOpTime());
            }
        } catch (const DBException& ex) {
            if (ex.code() == ErrorCodes::IncompleteTransactionHistory) {
                result.hasIncompleteHistory = true;
                break;
            }

            throw;
        }
    }

    return result;
}

void updateSessionEntry(OperationContext* opCtx, const UpdateRequest& updateRequest) {
    // Current code only supports replacement update.
    dassert(UpdateDriver::isDocReplacement(updateRequest.getUpdates()));

    AutoGetCollection autoColl(opCtx, NamespaceString::kSessionTransactionsTableNamespace, MODE_IX);

    uassert(40527,
            str::stream() << "Unable to persist transaction state because the session transaction "
                             "collection is missing. This indicates that the "
                          << NamespaceString::kSessionTransactionsTableNamespace.ns()
                          << " collection has been manually deleted.",
            autoColl.getCollection());

    WriteUnitOfWork wuow(opCtx);

    auto collection = autoColl.getCollection();
    auto idIndex = collection->getIndexCatalog()->findIdIndex(opCtx);

    uassert(40672,
            str::stream() << "Failed to fetch _id index for "
                          << NamespaceString::kSessionTransactionsTableNamespace.ns(),
            idIndex);

    auto indexAccess = collection->getIndexCatalog()->getIndex(idIndex);
    // Since we are looking up a key inside the _id index, create a key object consisting of only
    // the _id field.
    auto idToFetch = updateRequest.getQuery().firstElement();
    auto toUpdateIdDoc = idToFetch.wrap();
    dassert(idToFetch.fieldNameStringData() == "_id"_sd);
    auto recordId = indexAccess->findSingle(opCtx, toUpdateIdDoc);
    auto startingSnapshotId = opCtx->recoveryUnit()->getSnapshotId();

    if (recordId.isNull()) {
        // Upsert case.
        auto status = collection->insertDocument(
            opCtx, InsertStatement(updateRequest.getUpdates()), nullptr, true, false);

        if (status == ErrorCodes::DuplicateKey) {
            throw WriteConflictException();
        }

        uassertStatusOK(status);
        wuow.commit();
        return;
    }

    auto originalRecordData = collection->getRecordStore()->dataFor(opCtx, recordId);
    auto originalDoc = originalRecordData.toBson();

    invariant(collection->getDefaultCollator() == nullptr);
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(opCtx, nullptr));

    auto matcher =
        fassert(40673, MatchExpressionParser::parse(updateRequest.getQuery(), std::move(expCtx)));
    if (!matcher->matchesBSON(originalDoc)) {
        // Document no longer match what we expect so throw WCE to make the caller re-examine.
        throw WriteConflictException();
    }

    OplogUpdateEntryArgs args;
    args.nss = NamespaceString::kSessionTransactionsTableNamespace;
    args.uuid = collection->uuid();
    args.update = updateRequest.getUpdates();
    args.criteria = toUpdateIdDoc;
    args.fromMigrate = false;

    collection->updateDocument(opCtx,
                               recordId,
                               Snapshotted<BSONObj>(startingSnapshotId, originalDoc),
                               updateRequest.getUpdates(),
                               true,   // enforceQuota
                               false,  // indexesAffected = false because _id is the only index
                               nullptr,
                               &args);

    wuow.commit();
}

/**
 * Returns a new oplog entry if the given entry has transaction state embedded within in.
 * The new oplog entry will contain the operation needed to replicate the transaction
 * table.
 * Returns boost::none if the given oplog doesn't have any transaction state or does not
 * support update to the transaction table.
 */
boost::optional<repl::OplogEntry> createMatchingTransactionTableUpdate(
    const repl::OplogEntry& entry) {
    auto sessionInfo = entry.getOperationSessionInfo();
    if (!sessionInfo.getTxnNumber()) {
        return boost::none;
    }

    // Do not write session table entries for applyOps, as multi-document transactions
    // and retryable writes do not work together.
    // TODO(SERVER-33501): Make multi-docunment transactions work with retryable writes.
    if (entry.isCommand() && entry.getCommandType() == repl::OplogEntry::CommandType::kApplyOps) {
        return boost::none;
    }

    invariant(sessionInfo.getSessionId());
    invariant(entry.getWallClockTime());

    const auto updateBSON = [&] {
        SessionTxnRecord newTxnRecord;
        newTxnRecord.setSessionId(*sessionInfo.getSessionId());
        newTxnRecord.setTxnNum(*sessionInfo.getTxnNumber());
        newTxnRecord.setLastWriteOpTime(entry.getOpTime());
        newTxnRecord.setLastWriteDate(*entry.getWallClockTime());
        return newTxnRecord.toBSON();
    }();

    return repl::OplogEntry(
        entry.getOpTime(),
        0,  // hash
        repl::OpTypeEnum::kUpdate,
        NamespaceString::kSessionTransactionsTableNamespace,
        boost::none,  // uuid
        false,        // fromMigrate
        repl::OplogEntry::kOplogVersion,
        updateBSON,
        BSON(SessionTxnRecord::kSessionIdFieldName << sessionInfo.getSessionId()->toBSON()),
        {},    // sessionInfo
        true,  // upsert
        *entry.getWallClockTime(),
        boost::none,  // statementId
        boost::none,  // prevWriteOpTime
        boost::none,  // preImangeOpTime
        boost::none   // postImageOpTime
        );
}

// Failpoint which allows different failure actions to happen after each write. Supports the
// parameters below, which can be combined with each other (unless explicitly disallowed):
//
// closeConnection (bool, default = true): Closes the connection on which the write was executed.
// failBeforeCommitExceptionCode (int, default = not specified): If set, the specified exception
//      code will be thrown, which will cause the write to not commit; if not specified, the write
//      will be allowed to commit.
MONGO_FP_DECLARE(onPrimaryTransactionalWrite);

// Failpoint which will pause an operation just after allocating a point-in-time storage engine
// transaction.
MONGO_FP_DECLARE(hangAfterPreallocateSnapshot);
}  // namespace

const BSONObj Session::kDeadEndSentinel(BSON("$incompleteOplogHistory" << 1));

Session::Session(LogicalSessionId sessionId) : _sessionId(std::move(sessionId)) {}

void Session::refreshFromStorageIfNeeded(OperationContext* opCtx) {
    invariant(!opCtx->lockState()->isLocked());
    invariant(repl::ReadConcernArgs::get(opCtx).getLevel() ==
              repl::ReadConcernLevel::kLocalReadConcern);

    stdx::unique_lock<stdx::mutex> ul(_mutex);

    while (!_isValid) {
        const int numInvalidations = _numInvalidations;

        ul.unlock();

        auto activeTxnHistory = fetchActiveTransactionHistory(opCtx, _sessionId);

        ul.lock();

        // Protect against concurrent refreshes or invalidations
        if (!_isValid && _numInvalidations == numInvalidations) {
            _isValid = true;
            _lastWrittenSessionRecord = std::move(activeTxnHistory.lastTxnRecord);

            if (_lastWrittenSessionRecord) {
                _activeTxnNumber = _lastWrittenSessionRecord->getTxnNum();
                _activeTxnCommittedStatements = std::move(activeTxnHistory.committedStatements);
                _hasIncompleteHistory = activeTxnHistory.hasIncompleteHistory;
            }

            break;
        }
    }
}

void Session::beginOrContinueTxn(OperationContext* opCtx,
                                 TxnNumber txnNumber,
                                 boost::optional<bool> autocommit) {
    invariant(!opCtx->lockState()->isLocked());

    stdx::lock_guard<stdx::mutex> lg(_mutex);
    _beginOrContinueTxn(lg, txnNumber, autocommit);
}

void Session::beginOrContinueTxnOnMigration(OperationContext* opCtx, TxnNumber txnNumber) {
    invariant(!opCtx->lockState()->isLocked());

    stdx::lock_guard<stdx::mutex> lg(_mutex);
    _beginOrContinueTxnOnMigration(lg, txnNumber);
}


void Session::onWriteOpCompletedOnPrimary(OperationContext* opCtx,
                                          TxnNumber txnNumber,
                                          std::vector<StmtId> stmtIdsWritten,
                                          const repl::OpTime& lastStmtIdWriteOpTime,
                                          Date_t lastStmtIdWriteDate) {
    invariant(opCtx->lockState()->inAWriteUnitOfWork());

    stdx::unique_lock<stdx::mutex> ul(_mutex);
    // Multi-document transactions currently do not write to the transaction table.
    // TODO(SERVER-32323): Update transaction table appropriately when a transaction commits.
    if (!_autocommit)
        return;

    // Sanity check that we don't double-execute statements
    for (const auto stmtId : stmtIdsWritten) {
        const auto stmtOpTime = _checkStatementExecuted(ul, txnNumber, stmtId);
        if (stmtOpTime) {
            fassertOnRepeatedExecution(
                _sessionId, txnNumber, stmtId, *stmtOpTime, lastStmtIdWriteOpTime);
        }
    }

    const auto updateRequest =
        _makeUpdateRequest(ul, txnNumber, lastStmtIdWriteOpTime, lastStmtIdWriteDate);

    ul.unlock();

    repl::UnreplicatedWritesBlock doNotReplicateWrites(opCtx);

    updateSessionEntry(opCtx, updateRequest);
    _registerUpdateCacheOnCommit(
        opCtx, txnNumber, std::move(stmtIdsWritten), lastStmtIdWriteOpTime);
}

bool Session::onMigrateBeginOnPrimary(OperationContext* opCtx, TxnNumber txnNumber, StmtId stmtId) {
    beginOrContinueTxnOnMigration(opCtx, txnNumber);

    try {
        if (checkStatementExecuted(opCtx, txnNumber, stmtId)) {
            return false;
        }
    } catch (const DBException& ex) {
        // If the transaction chain was truncated on the recipient shard, then we
        // are most likely copying from a session that hasn't been touched on the
        // recipient shard for a very long time but could be recent on the donor.
        // We continue copying regardless to get the entire transaction from the donor.
        if (ex.code() != ErrorCodes::IncompleteTransactionHistory) {
            throw;
        }
        if (stmtId == kIncompleteHistoryStmtId) {
            return false;
        }
    }

    return true;
}

void Session::onMigrateCompletedOnPrimary(OperationContext* opCtx,
                                          TxnNumber txnNumber,
                                          std::vector<StmtId> stmtIdsWritten,
                                          const repl::OpTime& lastStmtIdWriteOpTime,
                                          Date_t lastStmtIdWriteDate) {
    invariant(opCtx->lockState()->inAWriteUnitOfWork());

    stdx::unique_lock<stdx::mutex> ul(_mutex);

    _checkValid(ul);
    _checkIsActiveTransaction(ul, txnNumber);

    const auto updateRequest =
        _makeUpdateRequest(ul, txnNumber, lastStmtIdWriteOpTime, lastStmtIdWriteDate);

    ul.unlock();

    repl::UnreplicatedWritesBlock doNotReplicateWrites(opCtx);

    updateSessionEntry(opCtx, updateRequest);
    _registerUpdateCacheOnCommit(
        opCtx, txnNumber, std::move(stmtIdsWritten), lastStmtIdWriteOpTime);
}

void Session::invalidate() {
    stdx::lock_guard<stdx::mutex> lg(_mutex);
    _isValid = false;
    _numInvalidations++;

    _lastWrittenSessionRecord.reset();

    _activeTxnNumber = kUninitializedTxnNumber;
    _activeTxnCommittedStatements.clear();
    _hasIncompleteHistory = false;
}

repl::OpTime Session::getLastWriteOpTime(TxnNumber txnNumber) const {
    stdx::lock_guard<stdx::mutex> lg(_mutex);
    _checkValid(lg);
    _checkIsActiveTransaction(lg, txnNumber);

    if (!_lastWrittenSessionRecord || _lastWrittenSessionRecord->getTxnNum() != txnNumber)
        return {};

    return _lastWrittenSessionRecord->getLastWriteOpTime();
}

boost::optional<repl::OplogEntry> Session::checkStatementExecuted(OperationContext* opCtx,
                                                                  TxnNumber txnNumber,
                                                                  StmtId stmtId) const {
    const auto stmtTimestamp = [&] {
        stdx::lock_guard<stdx::mutex> lg(_mutex);
        return _checkStatementExecuted(lg, txnNumber, stmtId);
    }();

    if (!stmtTimestamp)
        return boost::none;

    TransactionHistoryIterator txnIter(*stmtTimestamp);
    while (txnIter.hasNext()) {
        const auto entry = txnIter.next(opCtx);
        invariant(entry.getStatementId());
        if (*entry.getStatementId() == stmtId)
            return entry;
    }

    MONGO_UNREACHABLE;
}

bool Session::checkStatementExecutedNoOplogEntryFetch(TxnNumber txnNumber, StmtId stmtId) const {
    stdx::lock_guard<stdx::mutex> lg(_mutex);
    return bool(_checkStatementExecuted(lg, txnNumber, stmtId));
}

void Session::_beginOrContinueTxn(WithLock wl,
                                  TxnNumber txnNumber,
                                  boost::optional<bool> autocommit) {
    _checkValid(wl);
    _checkTxnValid(wl, txnNumber);

    if (txnNumber == _activeTxnNumber) {
        // Continuing an existing transaction.
        uassert(ErrorCodes::IllegalOperation,
                "Specifying 'autocommit' is only allowed at the beginning of a transaction",
                autocommit == boost::none);

        return;
    }

    // Start a new transaction with an autocommit field
    _setActiveTxn(wl, txnNumber);
    _autocommit = (autocommit != boost::none) ? *autocommit : true;  // autocommit defaults to true
    _txnState = _autocommit ? MultiDocumentTransactionState::kNone
                            : MultiDocumentTransactionState::kInProgress;
    invariant(_transactionOperations.empty());
}

void Session::_checkTxnValid(WithLock, TxnNumber txnNumber) const {
    uassert(ErrorCodes::TransactionTooOld,
            str::stream() << "Cannot start transaction " << txnNumber << " on session "
                          << getSessionId()
                          << " because a newer transaction "
                          << _activeTxnNumber
                          << " has already started.",
            txnNumber >= _activeTxnNumber);
    // TODO(SERVER-33432): Auto-abort an old transaction when a new one starts instead of asserting.
    uassert(40691,
            str::stream() << "Cannot start transaction " << txnNumber << " on session "
                          << getSessionId()
                          << " because a multi-document transaction "
                          << _activeTxnNumber
                          << " is in progress.",
            txnNumber == _activeTxnNumber ||
                (_transactionOperations.empty() &&
                 _txnState != MultiDocumentTransactionState::kCommitting));
}

Session::TxnResources::TxnResources(OperationContext* opCtx) {
    opCtx->getWriteUnitOfWork()->release();
    opCtx->setWriteUnitOfWork(nullptr);

    _locker = opCtx->swapLockState(stdx::make_unique<DefaultLockerImpl>());
    _locker->releaseTicket();

    _recoveryUnit = std::unique_ptr<RecoveryUnit>(opCtx->releaseRecoveryUnit());
    opCtx->setRecoveryUnit(opCtx->getServiceContext()->getGlobalStorageEngine()->newRecoveryUnit(),
                           OperationContext::kNotInUnitOfWork);

    _readConcernArgs = repl::ReadConcernArgs::get(opCtx);
}

Session::TxnResources::~TxnResources() {
    if (!_released && _recoveryUnit) {
        // This should only be reached when aborting a transaction that isn't active, i.e.
        // when starting a new transaction before completing an old one.  So we should
        // be at WUOW nesting level 1 (only the top level WriteUnitOfWork).
        _locker->endWriteUnitOfWork();
        invariant(!_locker->inAWriteUnitOfWork());
        _recoveryUnit->abortUnitOfWork();
    }
}

void Session::TxnResources::release(OperationContext* opCtx) {
    // Perform operations that can fail the release before marking the TxnResources as released.
    auto& readConcernArgs = repl::ReadConcernArgs::get(opCtx);
    uassert(ErrorCodes::InvalidOptions,
            "Only the first command in a transaction may specify a readConcern",
            readConcernArgs.isEmpty());

    _locker->reacquireTicket(opCtx);

    invariant(!_released);
    _released = true;

    // We intentionally do not capture the return value of swapLockState(), which is just an empty
    // locker. At the end of the operation, if the transaction is not complete, we will stash the
    // operation context's locker and replace it with a new empty locker.
    invariant(opCtx->lockState()->getClientState() == Locker::ClientState::kInactive);
    opCtx->swapLockState(std::move(_locker));

    opCtx->setRecoveryUnit(_recoveryUnit.release(),
                           OperationContext::RecoveryUnitState::kNotInUnitOfWork);

    opCtx->setWriteUnitOfWork(WriteUnitOfWork::createForSnapshotResume(opCtx));

    // 'readConcernArgs' is a mutable reference to the ReadConcernArgs decoration on opCtx.
    readConcernArgs = _readConcernArgs;
}

void Session::stashTransactionResources(OperationContext* opCtx) {
    invariant(opCtx->getTxnNumber());

    // We must lock the Client to change the Locker on the OperationContext and the Session mutex to
    // access Session state. We must lock the Client before the Session mutex, since the Client
    // effectively owns the Session. That is, a user might lock the Client to ensure it doesn't go
    // away, and then lock the Session owned by that client. We rely on the fact that we are not
    // using the  DefaultLockerImpl to avoid deadlock.

    invariant(!isMMAPV1());
    stdx::lock_guard<Client> lk(*opCtx->getClient());
    stdx::unique_lock<stdx::mutex> lg(_mutex);

    if (*opCtx->getTxnNumber() != _activeTxnNumber) {
        // The session is checked out, so _activeTxnNumber cannot advance due to a user operation.
        // However, when a chunk is migrated, session and transaction information is copied from the
        // donor shard to the recipient. This occurs outside of the check-out mechanism and can lead
        // to a higher _activeTxnNumber during the lifetime of a checkout. If that occurs, we abort
        // the current transaction. Note that it would indicate a user bug to have a newer
        // transaction on one shard while an older transaction is still active on another shard.
        uasserted(ErrorCodes::TransactionAborted,
                  str::stream() << "Transaction aborted. Active txnNumber is now "
                                << _activeTxnNumber);
    }

    if (_txnState != MultiDocumentTransactionState::kInProgress &&
        _txnState != MultiDocumentTransactionState::kInSnapshotRead) {
        // Not in a multi-document transaction or snapshot read: nothing to do.
        return;
    }

    if (_txnState == MultiDocumentTransactionState::kInSnapshotRead && !opCtx->hasStashedCursor()) {
        // The snapshot read is complete.
        invariant(opCtx->getWriteUnitOfWork());
        // We cannot hold the session lock during the commit, or a deadlock results.
        _txnState = MultiDocumentTransactionState::kCommitting;
        lg.unlock();
        opCtx->getWriteUnitOfWork()->commit();
        opCtx->setWriteUnitOfWork(nullptr);
        lg.lock();
        _txnState = MultiDocumentTransactionState::kCommitted;
        return;
    }

    invariant(!_txnResourceStash);
    _txnResourceStash = TxnResources(opCtx);
}

void Session::unstashTransactionResources(OperationContext* opCtx) {
    invariant(opCtx->getTxnNumber());

    // If the storage engine is mmapv1, it is not safe to lock both the Client and the Session
    // mutex. This is fine because mmapv1 does not support transactions.
    if (isMMAPV1()) {
        return;
    }

    bool snapshotPreallocated = false;
    {
        // We must lock the Client to change the Locker on the OperationContext and the Session
        // mutex to access Session state. We must lock the Client before the Session mutex, since
        // the Client effectively owns the Session. That is, a user might lock the Client to ensure
        // it doesn't go away, and then lock the Session owned by that client.
        stdx::lock_guard<Client> lk(*opCtx->getClient());
        stdx::lock_guard<stdx::mutex> lg(_mutex);
        if (opCtx->getTxnNumber() < _activeTxnNumber) {
            // The session is checked out, so _activeTxnNumber cannot advance due to a user
            // operation.
            // However, when a chunk is migrated, session and transaction information is copied from
            // the donor shard to the recipient. This occurs outside of the check-out mechanism and
            // can lead to a higher _activeTxnNumber during the lifetime of a checkout. If that
            // occurs, we abort the current transaction. Note that it would indicate a user bug to
            // have a newer transaction on one shard while an older transaction is still active on
            // another shard.
            _releaseStashedTransactionResources(lg);
            uasserted(ErrorCodes::TransactionAborted,
                      str::stream() << "Transaction aborted. Active txnNumber is now "
                                    << _activeTxnNumber);
            return;
        }

        if (_txnResourceStash) {
            invariant(_txnState != MultiDocumentTransactionState::kNone);
            _txnResourceStash->release(opCtx);
            _txnResourceStash = boost::none;
        } else {
            auto readConcernArgs = repl::ReadConcernArgs::get(opCtx);
            if (readConcernArgs.getLevel() == repl::ReadConcernLevel::kSnapshotReadConcern ||
                _txnState == MultiDocumentTransactionState::kInProgress) {
                opCtx->setWriteUnitOfWork(std::make_unique<WriteUnitOfWork>(opCtx));

                // Storage engine transactions may be started in a lazy manner. By explicitly
                // starting here we ensure that a point-in-time snapshot is established during the
                // first operation of a transaction.
                opCtx->recoveryUnit()->preallocateSnapshot();
                snapshotPreallocated = true;

                if (_txnState != MultiDocumentTransactionState::kInProgress) {
                    invariant(_txnState == MultiDocumentTransactionState::kNone);
                    _txnState = MultiDocumentTransactionState::kInSnapshotRead;
                }
            }
        }
    }

    if (snapshotPreallocated) {
        // The Client lock must not be held when executing this failpoint as it will block currentOp
        // execution.
        MONGO_FAIL_POINT_PAUSE_WHILE_SET(hangAfterPreallocateSnapshot);
    }
}

void Session::abortIfSnapshotRead(TxnNumber txnNumber) {
    stdx::lock_guard<stdx::mutex> lg(_mutex);
    if (_activeTxnNumber == txnNumber && _autocommit) {
        _releaseStashedTransactionResources(lg);
        _txnState = MultiDocumentTransactionState::kAborted;
    }
}

void Session::abortTransaction() {
    stdx::lock_guard<stdx::mutex> lg(_mutex);
    _releaseStashedTransactionResources(lg);
    _txnState = MultiDocumentTransactionState::kAborted;
}

void Session::_releaseStashedTransactionResources(WithLock wl) {
    _txnResourceStash = boost::none;
    _transactionOperations.clear();
    _txnState = MultiDocumentTransactionState::kNone;
}

void Session::_beginOrContinueTxnOnMigration(WithLock wl, TxnNumber txnNumber) {
    _checkValid(wl);
    _checkTxnValid(wl, txnNumber);

    // Check for continuing an existing transaction
    if (txnNumber == _activeTxnNumber)
        return;

    _setActiveTxn(wl, txnNumber);
}

void Session::_setActiveTxn(WithLock, TxnNumber txnNumber) {
    _activeTxnNumber = txnNumber;
    _activeTxnCommittedStatements.clear();
    _hasIncompleteHistory = false;
    _txnResourceStash = boost::none;
}

void Session::addTransactionOperation(OperationContext* opCtx,
                                      const repl::ReplOperation& operation) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_txnState == MultiDocumentTransactionState::kInProgress);
    invariant(!_autocommit && _activeTxnNumber != kUninitializedTxnNumber);
    invariant(opCtx->lockState()->inAWriteUnitOfWork());
    if (_transactionOperations.empty()) {
        auto txnNumberCompleting = _activeTxnNumber;
        opCtx->recoveryUnit()->onRollback([this, txnNumberCompleting] {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            invariant(_activeTxnNumber == txnNumberCompleting);
            invariant(_txnState != MultiDocumentTransactionState::kCommitted);
            _transactionOperations.clear();
            _txnState = MultiDocumentTransactionState::kAborted;
        });
        opCtx->recoveryUnit()->onCommit([this, txnNumberCompleting] {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            invariant(_activeTxnNumber == txnNumberCompleting);
            invariant(_txnState == MultiDocumentTransactionState::kCommitting ||
                      _txnState == MultiDocumentTransactionState::kCommitted);
            _txnState = MultiDocumentTransactionState::kCommitted;
        });
    }
    _transactionOperations.push_back(operation);
}

std::vector<repl::ReplOperation> Session::endTransactionAndRetrieveOperations() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(!_autocommit);
    invariant(_txnState == MultiDocumentTransactionState::kInProgress);
    // If _transactionOperations is empty, we will not see a commit because the write unit
    // of work is empty.
    _txnState = _transactionOperations.empty() ? MultiDocumentTransactionState::kCommitted
                                               : MultiDocumentTransactionState::kCommitting;
    return std::move(_transactionOperations);
}

void Session::_checkValid(WithLock) const {
    uassert(ErrorCodes::ConflictingOperationInProgress,
            str::stream() << "Session " << getSessionId()
                          << " was concurrently modified and the operation must be retried.",
            _isValid);
}

void Session::_checkIsActiveTransaction(WithLock, TxnNumber txnNumber) const {
    uassert(ErrorCodes::ConflictingOperationInProgress,
            str::stream() << "Cannot perform retryability check for transaction " << txnNumber
                          << " on session "
                          << getSessionId()
                          << " because a different transaction "
                          << _activeTxnNumber
                          << " is now active.",
            txnNumber == _activeTxnNumber);
}

boost::optional<repl::OpTime> Session::_checkStatementExecuted(WithLock wl,
                                                               TxnNumber txnNumber,
                                                               StmtId stmtId) const {
    _checkValid(wl);
    _checkIsActiveTransaction(wl, txnNumber);

    const auto it = _activeTxnCommittedStatements.find(stmtId);
    if (it == _activeTxnCommittedStatements.end()) {
        uassert(ErrorCodes::IncompleteTransactionHistory,
                str::stream() << "Incomplete history detected for transaction " << txnNumber
                              << " on session "
                              << _sessionId.toBSON(),
                !_hasIncompleteHistory);

        return boost::none;
    }

    invariant(_lastWrittenSessionRecord);
    invariant(_lastWrittenSessionRecord->getTxnNum() == txnNumber);

    return it->second;
}

UpdateRequest Session::_makeUpdateRequest(WithLock,
                                          TxnNumber newTxnNumber,
                                          const repl::OpTime& newLastWriteOpTime,
                                          Date_t newLastWriteDate) const {
    UpdateRequest updateRequest(NamespaceString::kSessionTransactionsTableNamespace);

    const auto updateBSON = [&] {
        SessionTxnRecord newTxnRecord;
        newTxnRecord.setSessionId(_sessionId);
        newTxnRecord.setTxnNum(newTxnNumber);
        newTxnRecord.setLastWriteOpTime(newLastWriteOpTime);
        newTxnRecord.setLastWriteDate(newLastWriteDate);
        return newTxnRecord.toBSON();
    }();
    updateRequest.setUpdates(updateBSON);
    updateRequest.setQuery(BSON(SessionTxnRecord::kSessionIdFieldName << _sessionId.toBSON()));
    updateRequest.setUpsert(true);

    return updateRequest;
}

void Session::_registerUpdateCacheOnCommit(OperationContext* opCtx,
                                           TxnNumber newTxnNumber,
                                           std::vector<StmtId> stmtIdsWritten,
                                           const repl::OpTime& lastStmtIdWriteOpTime) {
    opCtx->recoveryUnit()->onCommit(
        [ this, newTxnNumber, stmtIdsWritten = std::move(stmtIdsWritten), lastStmtIdWriteOpTime ] {
            RetryableWritesStats::get(getGlobalServiceContext())
                ->incrementTransactionsCollectionWriteCount();

            stdx::lock_guard<stdx::mutex> lg(_mutex);

            if (!_isValid)
                return;

            // The cache of the last written record must always be advanced after a write so that
            // subsequent writes have the correct point to start from.
            if (!_lastWrittenSessionRecord) {
                _lastWrittenSessionRecord.emplace();

                _lastWrittenSessionRecord->setSessionId(_sessionId);
                _lastWrittenSessionRecord->setTxnNum(newTxnNumber);
                _lastWrittenSessionRecord->setLastWriteOpTime(lastStmtIdWriteOpTime);
            } else {
                if (newTxnNumber > _lastWrittenSessionRecord->getTxnNum())
                    _lastWrittenSessionRecord->setTxnNum(newTxnNumber);

                if (lastStmtIdWriteOpTime > _lastWrittenSessionRecord->getLastWriteOpTime())
                    _lastWrittenSessionRecord->setLastWriteOpTime(lastStmtIdWriteOpTime);
            }

            if (newTxnNumber > _activeTxnNumber) {
                // This call is necessary in order to advance the txn number and reset the cached
                // state in the case where just before the storage transaction commits, the cache
                // entry gets invalidated and immediately refreshed while there were no writes for
                // newTxnNumber yet. In this case _activeTxnNumber will be less than newTxnNumber
                // and we will fail to update the cache even though the write was successful.
                _beginOrContinueTxn(lg, newTxnNumber, boost::none);
            }

            if (newTxnNumber == _activeTxnNumber) {
                for (const auto stmtId : stmtIdsWritten) {
                    if (stmtId == kIncompleteHistoryStmtId) {
                        _hasIncompleteHistory = true;
                        continue;
                    }

                    const auto insertRes =
                        _activeTxnCommittedStatements.emplace(stmtId, lastStmtIdWriteOpTime);
                    if (!insertRes.second) {
                        const auto& existingOpTime = insertRes.first->second;
                        fassertOnRepeatedExecution(_sessionId,
                                                   newTxnNumber,
                                                   stmtId,
                                                   existingOpTime,
                                                   lastStmtIdWriteOpTime);
                    }
                }
            }
        });

    MONGO_FAIL_POINT_BLOCK(onPrimaryTransactionalWrite, customArgs) {
        const auto& data = customArgs.getData();

        const auto closeConnectionElem = data["closeConnection"];
        if (closeConnectionElem.eoo() || closeConnectionElem.Bool()) {
            opCtx->getClient()->session()->end();
        }

        const auto failBeforeCommitExceptionElem = data["failBeforeCommitExceptionCode"];
        if (!failBeforeCommitExceptionElem.eoo()) {
            const auto failureCode = ErrorCodes::Error(int(failBeforeCommitExceptionElem.Number()));
            uasserted(failureCode,
                      str::stream() << "Failing write for " << _sessionId << ":" << newTxnNumber
                                    << " due to failpoint. The write must not be reflected.");
        }
    }
}

std::vector<repl::OplogEntry> Session::addOpsForReplicatingTxnTable(
    const std::vector<repl::OplogEntry>& ops) {
    std::vector<repl::OplogEntry> newOps;

    for (auto&& op : ops) {
        newOps.push_back(op);

        if (auto updateTxnTableOp = createMatchingTransactionTableUpdate(op)) {
            newOps.push_back(*updateTxnTableOp);
        }
    }

    return newOps;
}

}  // namespace mongo
