// SERVER-23310: Add $arrayToObject aggregation expression.
load('jstests/aggregation/extras/utils.js');

(function (){
    var coll = db.array_to_object_expr;
    coll.drop();
    var array_to_object_expr =  { $project: {
                                          collapsed: {$arrayToObject: '$expanded'}
                                }};

    assert.writeOK(coll.insert({_id: 0, expanded:[ ["price", 24], ["item", "apple"] ]} ));
    var result = coll.aggregate([ array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 0, collapsed: {"price": 24, "item": "apple"} }]);

    // Non-array types.
    assert.writeOK(coll.insert({_id: 1, expanded: "string"}));
    assertErrorCode(coll, [ {$match: {_id: 1}}, array_to_object_expr ], 40372);

    assert.writeOK(coll.insert({_id: 2, expanded: ObjectId()}));
    assertErrorCode(coll, [ {$match: {_id: 2}}, array_to_object_expr ], 40372);


    assert.writeOK(coll.insert({_id: 3, expanded: NumberLong(0)}));
    assertErrorCode(coll, [ {$match: {_id: 3}}, array_to_object_expr ], 40372);


    assert.writeOK(coll.insert({_id: 4, expanded: null}));
    result = coll.aggregate([ {$match: {_id: 4}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 4, collapsed: null }] );

    assert.writeOK(coll.insert({_id: 5, expanded: undefined}));
    result = coll.aggregate([ {$match: {_id: 5}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 5, collapsed: null }] );

    assert.writeOK(coll.insert({_id: 6, expanded: NaN}));
    assertErrorCode(coll, [ {$match: {_id: 6}}, array_to_object_expr ], 40372);

    // Array types.
    assert.writeOK(coll.insert({_id: 7, expanded: []}));
    result = coll.aggregate([ {$match: {_id: 7}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 7, collapsed: {} }] );    


    assert.writeOK(coll.insert({_id: 8, expanded: [0]}));
    assertErrorCode(coll, [ {$match: {_id: 8}}, array_to_object_expr ], 40373);

    assert.writeOK(coll.insert({_id: 9, expanded: [ ["missing_value"] ]}));
    assertErrorCode(coll, [ {$match: {_id: 9}}, array_to_object_expr ], 40374);
    
    assert.writeOK(coll.insert({_id: 12, expanded: [ [123, 12] ]}));
    assertErrorCode(coll, [ {$match: {_id: 12}}, array_to_object_expr ], 40375);

    assert.writeOK(coll.insert({_id: 10, expanded: [ ["key", "value", "offset"] ]}));
    assertErrorCode(coll, [ {$match: {_id: 10}}, array_to_object_expr ], 40374);

    // Document types.
    assert.writeOK(coll.insert({_id: 11, expanded: {y: []}}));
    assertErrorCode(coll, [ {$match: {_id: 11}}, array_to_object_expr ], 40372);

}());