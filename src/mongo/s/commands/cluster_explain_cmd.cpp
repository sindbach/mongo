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

#include "mongo/client/dbclientinterface.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/explain.h"
#include "mongo/s/query/cluster_find.h"

namespace mongo {
namespace {

/**
 * Implements the explain command on mongos.
 *
 * "Old-style" explains (i.e. queries which have the $explain flag set), do not run
 * through this path. Such explains will be supported for backwards compatibility,
 * and must succeed in multiversion clusters.
 *
 * "New-style" explains use the explain command. When the explain command is routed
 * through mongos, it is forwarded to all relevant shards. If *any* shard does not
 * support a new-style explain, then the entire explain will fail (i.e. new-style
 * explains cannot be used in multiversion clusters).
 */

class ClusterExplainCmd final : public Command {
public:
    ClusterExplainCmd() : Command("explain") {}

    std::unique_ptr<CommandInvocation> parse(OperationContext* opCtx,
                                             const OpMsgRequest& request) override;

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    /**
     * Running an explain on a secondary requires explicitly setting slaveOk.
     */
    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kOptIn;
    }

    bool maintenanceOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return false;
    }

    std::string help() const override {
        return "explain database reads and writes";
    }

    /**
     * You are authorized to run an explain if you are authorized to run
     * the command that you are explaining. The auth check is performed recursively
     * on the nested command.
     */
    Status checkAuthForRequest(OperationContext* opCtx,
                               const OpMsgRequest& request) const override {
        CommandHelpers::uassertNoDocumentSequences(getName(), request);
        const std::string dbname = request.getDatabase().toString();
        const BSONObj& cmdObj = request.body;

        if (Object != cmdObj.firstElement().type()) {
            return Status(ErrorCodes::BadValue, "explain command requires a nested object");
        }

        BSONObj explainedObj = cmdObj.firstElement().Obj();

        auto explainedCommand = CommandHelpers::findCommand(explainedObj.firstElementFieldName());
        if (!explainedCommand) {
            return Status(ErrorCodes::CommandNotFound,
                          str::stream() << "unknown command: "
                                        << explainedObj.firstElementFieldName());
        }

        return explainedCommand->checkAuthForRequest(
            opCtx, OpMsgRequest::fromDBAndBody(dbname, std::move(explainedObj)));
    }

private:
    class Invocation;
};

class ClusterExplainCmd::Invocation final : public CommandInvocation {
public:
    Invocation(ClusterExplainCmd* explainCommand,
               const OpMsgRequest& request,
               ExplainOptions::Verbosity verbosity,
               std::unique_ptr<OpMsgRequest> innerRequest,
               std::unique_ptr<CommandInvocation> innerInvocation)
        : CommandInvocation(explainCommand),
          _outerRequest{&request},
          _dbName{_outerRequest->getDatabase().toString()},
          _ns{command()->parseNs(_dbName, _outerRequest->body)},
          _verbosity{std::move(verbosity)},
          _innerRequest{std::move(innerRequest)},
          _innerInvocation{std::move(innerInvocation)} {}

private:
    void run(OperationContext* opCtx, CommandReplyBuilder* result) override {
        try {
            auto bob = result->getBodyBuilder();
            _innerInvocation->explain(opCtx, _verbosity, &bob);
        } catch (const ExceptionFor<ErrorCodes::Unauthorized>&) {
            CommandHelpers::logAuthViolation(
                opCtx, command(), *_outerRequest, ErrorCodes::Unauthorized);
            throw;
        }
    }

    void explain(OperationContext* opCtx,
                 ExplainOptions::Verbosity verbosity,
                 BSONObjBuilder* result) override {
        uasserted(ErrorCodes::IllegalOperation, "Explain cannot explain itself.");
    }

    NamespaceString ns() const override {
        return _ns;
    }

    bool supportsWriteConcern() const override {
        return command()->supportsWriteConcern(_outerRequest->body);
    }

    Command::AllowedOnSecondary secondaryAllowed(ServiceContext* context) const override {
        return command()->secondaryAllowed(context);
    }

    void doCheckAuthorization(OperationContext* opCtx) const override {
        uassertStatusOK(command()->checkAuthForRequest(opCtx, *_outerRequest));
    }

    const ClusterExplainCmd* command() const {
        return static_cast<const ClusterExplainCmd*>(definition());
    }

    const OpMsgRequest* _outerRequest;
    const std::string _dbName;
    NamespaceString _ns;
    ExplainOptions::Verbosity _verbosity;
    std::unique_ptr<OpMsgRequest> _innerRequest;  // Lifespan must enclose that of _innerInvocation.
    std::unique_ptr<CommandInvocation> _innerInvocation;
};

std::unique_ptr<CommandInvocation> ClusterExplainCmd::parse(OperationContext* opCtx,
                                                            const OpMsgRequest& request) {
    CommandHelpers::uassertNoDocumentSequences(getName(), request);
    std::string dbName = request.getDatabase().toString();
    const BSONObj& cmdObj = request.body;
    ExplainOptions::Verbosity verbosity = uassertStatusOK(ExplainOptions::parseCmdBSON(cmdObj));

    // This is the nested command which we are explaining. We need to propagate generic
    // arguments into the inner command since it is what is passed to the virtual
    // Command::explain() method.
    const BSONObj explainedObj = [&] {
        const auto innerObj = cmdObj.firstElement().Obj();
        if (auto innerDb = innerObj["$db"]) {
            uassert(ErrorCodes::InvalidNamespace,
                    str::stream() << "Mismatched $db in explain command. Expected " << dbName
                                  << " but got "
                                  << innerDb.checkAndGetStringData(),
                    innerDb.checkAndGetStringData() == dbName);
        }

        BSONObjBuilder bob;
        bob.appendElements(innerObj);
        for (auto outerElem : cmdObj) {
            // If the argument is in both the inner and outer command, we currently let the
            // inner version take precedence.
            const auto name = outerElem.fieldNameStringData();
            if (CommandHelpers::isGenericArgument(name) && !innerObj.hasField(name)) {
                bob.append(outerElem);
            }
        }
        return bob.obj();
    }();

    const std::string cmdName = explainedObj.firstElementFieldName();
    auto explainedCommand = CommandHelpers::findCommand(cmdName);
    uassert(ErrorCodes::CommandNotFound,
            str::stream() << "Explain failed due to unknown command: " << cmdName,
            explainedCommand != nullptr);

    // Actually call the nested command's explain(...) method.
    auto innerRequest = std::make_unique<OpMsgRequest>(OpMsg{explainedObj});
    auto innerInvocation = explainedCommand->parse(opCtx, *innerRequest);
    return stdx::make_unique<Invocation>(
        this, request, std::move(verbosity), std::move(innerRequest), std::move(innerInvocation));
}

ClusterExplainCmd cmdExplainCluster;

}  // namespace
}  // namespace mongo
