// SERVER-18794: Add $objectToArray aggregation expression.
(function (){
    var coll = db.object_to_array_expr;
    coll.drop();

    // As description
    assert.writeOK(coll.insert({_id: 0, subDoc: { a: 1, b: 2, c: "string" } }));

    // Non-array types.
    assert.writeOK(coll.insert({_id: 1, subDoc: "string"}));
    assert.writeOK(coll.insert({_id: 2, subDoc: ObjectId()}));
    assert.writeOK(coll.insert({_id: 3, subDoc: NumberLong(0)}));
    assert.writeOK(coll.insert({_id: 4, subDoc: null}));
    assert.writeOK(coll.insert({_id: 5, subDoc: NaN}));
    assert.writeOK(coll.insert({_id: 6, subDoc: undefined}));

    // Array types.
    assert.writeOK(coll.insert({_id: 7, subDoc: []}));
    assert.writeOK(coll.insert({_id: 8, subDoc: [0]}));
    assert.writeOK(coll.insert({_id: 9, subDoc: ['string']}));

    // Document types.
    assert.writeOK(coll.insert({_id: 10, subDoc: {y: []}}));
    assert.writeOK(coll.insert({_id: 11, subDoc: { a:1, b: {d: "string"}, c: [1, 2] } }));

    var results = coll.aggregate([{$sort: {_id: 1}},
                                  {
                                      $project: {
                                          expanded: {$objectToArray: '$subDoc'}
                                      }
                                  },
                                 ]).toArray();

    var expectedResults = [
        {_id: 0, expanded: [["a", 1], ["b", 2], ["c", "string"]]},
        {_id: 1, expanded: []},
        {_id: 2, expanded: []},
        {_id: 3, expanded: []},
        {_id: 4, expanded: []},
        {_id: 5, expanded: []},
        {_id: 6, expanded: []},
        {_id: 7, expanded: []},
        {_id: 8, expanded: []},
        {_id: 9, expanded: []},
        {_id: 10, expanded: [["y", []]]},
        {_id: 11, expanded: [["a", 1], ["b", {d: "string"}], ["c", [1, 2]]]},
    ];

    assert.eq(results, expectedResults);
}());