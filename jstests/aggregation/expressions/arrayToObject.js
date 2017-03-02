// Tests for $arrayToObject aggregation expression.
(function() {
    "use strict";

    // For assertErrorCode().
    load('jstests/aggregation/extras/utils.js');

    let coll = db.array_to_object_expr;
    coll.drop();

    let array_to_object_expr = {$project: {collapsed: {$arrayToObject: '$expanded'}}};

    // $arrayToObject correctly converts a key-value pairs to an object.
    assert.writeOK(coll.insert({_id: 0, expanded: [["price", 24], ["item", "apple"]]}));
    let result = coll.aggregate([{$match: {_id: 0}}, array_to_object_expr]).toArray();
    assert.eq(result, [{_id: 0, collapsed: {"price": 24, "item": "apple"}}]);

    assert.writeOK(
        coll.insert({_id: 1, expanded: [{"k": "price", "v": 24}, {"k": "item", "v": "apple"}]}));
    result = coll.aggregate([{$match: {_id: 1}}, array_to_object_expr]).toArray();
    assert.eq(result, [{_id: 1, collapsed: {"price": 24, "item": "apple"}}]);

    assert.writeOK(
        coll.insert({_id: 2, expanded: [{"k": "price", "v": 24}, {"k": "item", "v": "apple"}]}));
    result =
        coll.aggregate([
                {$match: {_id: 2}},
                {
                  $project: {
                      collapsed:
                          {$arrayToObject: {$zip: {inputs: ["$expanded.k", "$expanded.v"]}}}
                  }
                }
            ])
            .toArray();
    assert.eq(result, [{_id: 2, collapsed: {"price": 24, "item": "apple"}}]);

    assert.writeOK(coll.insert({_id: 3, expanded: []}));
    result = coll.aggregate([{$match: {_id: 3}}, array_to_object_expr]).toArray();
    assert.eq(result, [{_id: 3, collapsed: {}}]);

    assert.writeOK(coll.insert({_id: 4}));
    result = coll.aggregate([{$match: {_id: 4}}, array_to_object_expr]).toArray();
    assert.eq(result, [{_id: 4, collapsed: null}]);

    // $arrayToObject outputs null on null-ish types.
    assert.writeOK(coll.insert({_id: 5, expanded: null}));
    result = coll.aggregate([{$match: {_id: 5}}, array_to_object_expr]).toArray();
    assert.eq(result, [{_id: 5, collapsed: null}]);

    assert.writeOK(coll.insert({_id: 6, expanded: undefined}));
    result = coll.aggregate([{$match: {_id: 6}}, array_to_object_expr]).toArray();
    assert.eq(result, [{_id: 6, collapsed: null}]);

    assert.writeOK(coll.insert({_id: 7, expanded: [{"k": "price", "v": 24}, ["item", "apple"]]}));
    assertErrorCode(coll, [{$match: {_id: 7}}, array_to_object_expr], 40391);

    assert.writeOK(coll.insert({_id: 8, expanded: [["item", "apple"], {"k": "price", "v": 24}]}));
    assertErrorCode(coll, [{$match: {_id: 8}}, array_to_object_expr], 40388);

    assert.writeOK(coll.insert({_id: 9, expanded: "string"}));
    assertErrorCode(coll, [{$match: {_id: 9}}, array_to_object_expr], 40386);

    assert.writeOK(coll.insert({_id: 10, expanded: ObjectId()}));
    assertErrorCode(coll, [{$match: {_id: 10}}, array_to_object_expr], 40386);

    assert.writeOK(coll.insert({_id: 11, expanded: NumberLong(0)}));
    assertErrorCode(coll, [{$match: {_id: 11}}, array_to_object_expr], 40386);

    assert.writeOK(coll.insert({_id: 12, expanded: [0]}));
    assertErrorCode(coll, [{$match: {_id: 12}}, array_to_object_expr], 40387);

    assert.writeOK(coll.insert({_id: 13, expanded: [["missing_value"]]}));
    assertErrorCode(coll, [{$match: {_id: 13}}, array_to_object_expr], 40389);

    assert.writeOK(coll.insert({_id: 14, expanded: [[321, 12]]}));
    assertErrorCode(coll, [{$match: {_id: 14}}, array_to_object_expr], 40390);

    assert.writeOK(coll.insert({_id: 15, expanded: [["key", "value", "offset"]]}));
    assertErrorCode(coll, [{$match: {_id: 15}}, array_to_object_expr], 40389);

    assert.writeOK(coll.insert({_id: 16, expanded: {y: []}}));
    assertErrorCode(coll, [{$match: {_id: 16}}, array_to_object_expr], 40386);

    assert.writeOK(coll.insert({_id: 17, expanded: [{y: "x", x: "y"}]}));
    assertErrorCode(coll, [{$match: {_id: 17}}, array_to_object_expr], 40393);

    assert.writeOK(coll.insert({_id: 18, expanded: [{k: "missing"}]}));
    assertErrorCode(coll, [{$match: {_id: 18}}, array_to_object_expr], 40392);

    assert.writeOK(coll.insert({_id: 19, expanded: [{k: 24, v: "string"}]}));
    assertErrorCode(coll, [{$match: {_id: 19}}, array_to_object_expr], 40394);

    assert.writeOK(coll.insert({_id: 20, expanded: [{y: "ignored", k: "item", v: "pear"}]}));
    assertErrorCode(coll, [{$match: {_id: 20}}, array_to_object_expr], 40392);

    assert.writeOK(coll.insert({_id: 21, expanded: NaN}));
    assertErrorCode(coll, [{$match: {_id: 21}}, array_to_object_expr], 40386);

}());
