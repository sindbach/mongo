/**
 * Test that explained aggregation commands behave correctly with the readConcern option.
 */
(function() {
    "use strict";

    load("jstests/multiVersion/libs/causal_consistency_helpers.js");

    // Skip this test if running with --nojournal and WiredTiger.
    if (jsTest.options().noJournal &&
        (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
        print("Skipping test because running WiredTiger without journaling isn't a valid" +
              " replica set configuration");
        return;
    }

    if (!supportsMajorityReadConcern()) {
        jsTestLog("Skipping test since storage engine doesn't support majority read concern.");
        return;
    }

    const rst = new ReplSetTest(
        {name: "aggExplainReadConcernSet", nodes: 1, nodeOptions: {enableMajorityReadConcern: ""}});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();
    let testDB = primary.getDB("test");
    let coll = testDB.agg_explain_read_concern;

    // Test that explain is legal with readConcern "local".
    assert.commandWorked(coll.explain().aggregate([], {readConcern: {level: "local"}}));
    assert.commandWorked(testDB.runCommand(
        {aggregate: coll.getName(), pipeline: [], explain: true, readConcern: {level: "local"}}));
    assert.commandWorked(testDB.runCommand({
        explain: {aggregate: coll.getName(), pipeline: [], cursor: {}},
        readConcern: {level: "local"}
    }));

    // Test that explain is illegal with other readConcern levels.
    // TODO SERVER-33354: Add "snapshot" once the aggregate command supports.
    let nonLocalReadConcerns = ["majority", "available", "linearizable"];
    nonLocalReadConcerns.forEach(function(readConcernLevel) {
        assert.throws(() => coll.explain().aggregate([], {readConcern: {level: readConcernLevel}}));

        let cmdRes = testDB.runCommand({
            aggregate: coll.getName(),
            pipeline: [],
            explain: true,
            readConcern: {level: readConcernLevel}
        });
        assert.commandFailedWithCode(cmdRes, ErrorCodes.InvalidOptions, tojson(cmdRes));
        let expectedErrStr = "aggregate command does not support non-local readConcern";
        assert.neq(cmdRes.errmsg.indexOf(expectedErrStr), -1, tojson(cmdRes));

        cmdRes = testDB.runCommand({
            explain: {aggregate: coll.getName(), pipeline: [], cursor: {}},
            readConcern: {level: readConcernLevel}
        });
        assert.commandFailedWithCode(cmdRes, ErrorCodes.InvalidOptions, tojson(cmdRes));
        expectedErrStr = "Command does not support read concern";
        assert.neq(cmdRes.errmsg.indexOf(expectedErrStr), -1, tojson(cmdRes));
    });

    rst.stopSet();
}());
