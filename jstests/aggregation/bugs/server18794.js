// SERVER-18794: Add $objectToArray aggregation expression.
load('jstests/aggregation/extras/utils.js');

(function (){
    var coll = db.object_to_array_expr;
    coll.drop();

    var object_to_array_expr =  { $project: {
                                          expanded: {$objectToArray: '$subDoc'}
                                }};
    // As description
    assert.writeOK(coll.insert({_id: 0, subDoc: { "a": 1, "b": 2, "c": "foo" } } ));
    var result = coll.aggregate([{$match: {_id: 0}}, object_to_array_expr ]).toArray();
    assert.eq(result, [{_id: 0, expanded: [["a", 1], ["b", 2], ["c", "foo"]]}]);

    // Non-document types.
    assert.writeOK(coll.insert({_id: 1, subDoc: "string"}));
    assertErrorCode(coll, [ {$match: {_id: 1}}, object_to_array_expr ], 40379);

    assert.writeOK(coll.insert({_id: 2, subDoc: ObjectId()}));
    assertErrorCode(coll, [ {$match: {_id: 2}}, object_to_array_expr ], 40379);

    assert.writeOK(coll.insert({_id: 3, subDoc: NumberLong(0)}));
    assertErrorCode(coll, [ {$match: {_id: 3}}, object_to_array_expr ], 40379);

    assert.writeOK(coll.insert({_id: 4, subDoc: null}));
    result = coll.aggregate([ {$match: {_id: 4}}, object_to_array_expr ]).toArray();
    assert.eq(result, [{_id: 4, expanded: null }] );

    assert.writeOK(coll.insert({_id: 5, subDoc: undefined}));
    result = coll.aggregate([ {$match: {_id: 5}}, object_to_array_expr ]).toArray();
    assert.eq(result, [{_id: 5, expanded: null }] );

    assert.writeOK(coll.insert({_id: 6, subDoc: NaN}));
    assertErrorCode(coll, [ {$match: {_id: 6}}, object_to_array_expr ], 40379);

    assert.writeOK(coll.insert({_id: 7, subDoc: []}));
    assertErrorCode(coll, [ {$match: {_id: 7}}, object_to_array_expr ], 40379);

    assert.writeOK(coll.insert({_id: 8, subDoc: [0]}));
    assertErrorCode(coll, [ {$match: {_id: 8}}, object_to_array_expr ], 40379);

    assert.writeOK(coll.insert({_id: 9, subDoc: ['string']}));
    assertErrorCode(coll, [ {$match: {_id: 9}}, object_to_array_expr ], 40379);

    // Document types.
    assert.writeOK(coll.insert({_id: 10, subDoc: {"y": []}}));
    result = coll.aggregate([ {$match: {_id: 10}}, object_to_array_expr ]).toArray();
    assert.eq(result, [{_id: 10,  expanded:[ ["y", [] ] ] }] );


    assert.writeOK(coll.insert({_id: 11, subDoc: {"a":1, "b": {"d":"string"}, "c":[1, 2]}}));
    result = coll.aggregate([ {$match: {_id: 11}}, object_to_array_expr ]).toArray();
    assert.eq(result, [{_id: 11,  expanded:[ ["a", 1], ["b", {"d": "string"}], ["c", [1, 2]] ] }] );

    coll.drop(); 

    // Turns to array from the root of the document
    assert.writeOK(coll.insert({_id: 0, "a": 1, "b": 2, "c": 3}))
    results = coll.aggregate([{$sort: {_id: 1}},
                                  {
                                      $project: {
                                          document: {$objectToArray: '$$ROOT'}
                                      }
                                  },
                                 ]).toArray();
    expectedResults = [
        {_id:0, document: [["_id", 0], ["a", 1], ["b", 2], ["c", 3]]}
    ];
    assert.eq(results, expectedResults);

}());