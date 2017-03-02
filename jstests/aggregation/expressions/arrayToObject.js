// SERVER-23310: Add $arrayToObject aggregation expression.
load('jstests/aggregation/extras/utils.js');

(function (){
    var coll = db.array_to_object_expr;
    coll.drop();
    var array_to_object_expr =  { $project: {
                                          collapsed: {$arrayToObject: '$expanded'}
                                }};

    assert.writeOK(coll.insert({_id: 0, expanded:[ ["price", 24], ["item", "apple"] ]} ));
    var result = coll.aggregate([ {$match: {_id:0}},array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 0, collapsed: {"price": 24, "item": "apple"} }]);

    assert.writeOK(coll.insert({_id: 1, expanded:[ {"k": "price", "v": 24}, {"k":"item", "v":"apple"}]} ));
    result = coll.aggregate([ {$match: {_id:1}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 1, collapsed: {"price": 24, "item": "apple"} }]);

    assert.writeOK(coll.insert({_id: 2, expanded: [ {"k": "price", "v": 24}, ["item", "apple"]] }));
    assertErrorCode(coll, [ {$match: {_id: 2}}, array_to_object_expr ], 40393);

    assert.writeOK(coll.insert({_id: 3, expanded: [ ["item", "apple"], {"k": "price", "v": 24}] }));
    assertErrorCode(coll, [ {$match: {_id: 3}}, array_to_object_expr ], 40390);


    assert.writeOK(coll.insert({_id: 10, expanded: "string"}));
    assertErrorCode(coll, [ {$match: {_id: 10}}, array_to_object_expr ], 40388);

    assert.writeOK(coll.insert({_id: 11, expanded: ObjectId()}));
    assertErrorCode(coll, [ {$match: {_id: 12}}, array_to_object_expr ], 40388);

    assert.writeOK(coll.insert({_id: 13, expanded: NumberLong(0)}));
    assertErrorCode(coll, [ {$match: {_id: 13}}, array_to_object_expr ], 40388);


    assert.writeOK(coll.insert({_id: 14, expanded: null}));
    result = coll.aggregate([ {$match: {_id: 14}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 14, collapsed: null }] );

    assert.writeOK(coll.insert({_id: 15, expanded: undefined}));
    result = coll.aggregate([ {$match: {_id: 15}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 15, collapsed: null }] );

    assert.writeOK(coll.insert({_id: 16, expanded: NaN}));
    assertErrorCode(coll, [ {$match: {_id: 16}}, array_to_object_expr ], 40388);

    // Array types
    assert.writeOK(coll.insert({_id: 20, expanded: []}));
    result = coll.aggregate([ {$match: {_id: 20}}, array_to_object_expr ]).toArray();
    assert.eq(result, [{_id: 20, collapsed: {} }] );    

    assert.writeOK(coll.insert({_id: 21, expanded: [0]}));
    assertErrorCode(coll, [ {$match: {_id: 21}}, array_to_object_expr ], 40389);

    assert.writeOK(coll.insert({_id: 22, expanded: [ ["missing_value"] ]}));
    assertErrorCode(coll, [ {$match: {_id: 22}}, array_to_object_expr ], 40391);
    
    assert.writeOK(coll.insert({_id: 23, expanded: [ [123, 12] ]}));
    assertErrorCode(coll, [ {$match: {_id: 23}}, array_to_object_expr ], 40392);

    assert.writeOK(coll.insert({_id: 24, expanded: [ ["key", "value", "offset"] ]}));
    assertErrorCode(coll, [ {$match: {_id: 24}}, array_to_object_expr ], 40391);

    // Document types.
    assert.writeOK(coll.insert({_id: 25, expanded: {y: []}}));
    assertErrorCode(coll, [ {$match: {_id: 25}}, array_to_object_expr ], 40388);

    assert.writeOK(coll.insert({_id: 26, expanded: [{y: "x"}] }));
    assertErrorCode(coll, [ {$match: {_id: 26}}, array_to_object_expr ], 40395);

    assert.writeOK(coll.insert({_id: 27, expanded: [{k: "missing"}] }));
    assertErrorCode(coll, [ {$match: {_id: 27}}, array_to_object_expr ], 40395);

    assert.writeOK(coll.insert({_id: 28, expanded: [{k: 23, v: "string"}] }));
    assertErrorCode(coll, [ {$match: {_id: 28}}, array_to_object_expr ], 40396);

    assert.writeOK(coll.insert({_id: 29, expanded: [{y: "ignored", k:"item", v:"pear"}] }));
    assertErrorCode(coll, [ {$match: {_id: 29}}, array_to_object_expr ], 40394);

}());