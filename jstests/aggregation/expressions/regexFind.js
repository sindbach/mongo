// Tests for $regexFind aggregation expression.
(function() {
    "use strict";

    // For assertErrorCode().
    load("jstests/aggregation/extras/utils.js");

    let coll = db.regex_find_expr;
    coll.drop();

    assert.writeOK(coll.insert({_id: 0, text:"Simple Example"}));

    let result = coll.aggregate([{$match:{_id:0}}, 
                                 {$project:{
                                    matches:{
                                        $regexFind:["$text", /(m(p))/]
                                }}}]).toArray();
    assert.eq(result, 
             [{"_id" : 0, "matches" : [{"match" : "mp",
                                        "idx" : 2,
                                        "captures" : ["mp", "p"]
    }]}]); 

    result = coll.aggregate([{$match:{_id:0}}, 
                             {$project:{
                               matches:{
                                       $regexFind:["$text", /(m(p))/g]
                            }}}]).toArray();
    assert.eq(result, 
             [{"_id" : 0, "matches" : [{"match" : "mp",
                                        "idx" : 2,
                                        "captures" : ["mp", "p"]
                                       },
                                       {"match" : "mp",
                                        "idx" : 10,
                                        "captures" : ["mp", "p"]
                                       }
    ]}]); 

    assert.writeOK(coll.insert({_id: 1, text:"Some text with no matches"}));
    result = coll.aggregate([{$match:{_id:0}}, 
                             {$project:{
                               matches:{
                                       $regexFind:["$text", /not present/]
                            }}}]).toArray();
    assert.eq(result, [{"_id" : 0, "matches" : []}]); 

    result = coll.aggregate([{$match:{_id:0}}, 
                            {$project:{
                            matches:{
                                $regexFind:["$text", {"regex" : /(m(p))/ } ]
                            }}}]).toArray();
    assert.eq(result, 
            [{"_id" : 0, "matches" : [{"match" : "mp",
                        "idx" : 2,
                        "captures" : ["mp", "p"]
    }]}]); 

    /*result = coll.aggregate([{$match:{_id:0}}, 
                            {$project:{
                            matches:{
                                $regexFind:["$text", {'$regex': 'm(p))', $options: 'i'} ]
                            }}}]).toArray();
    assert.eq(result, 
        [{"_id" : 0, "matches" : [{"match" : "mp",
            "idx" : 2,
            "captures" : ["mp", "p"]
    }]}]); */

    assert.writeOK(coll.insert({_id: 2, text:"Some field text with email norberto@mongodb.com"}));

    result = coll.aggregate([{$match:{_id:2}}, 
                             {$project:{
                                 matches:{
                                    $regexFind:["$text", 
                                                /([a-zA-Z0-9._-]+)@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+/]
                                }}}]).toArray();
    assert.eq(result, 
             [{"_id" : 2, "matches" : [{"match" : "norberto@mongodb.com",
                                      "idx" : 27,
                                      "captures" : ["norberto"]
    }]}]); 

    assert.writeOK(coll.insert({_id: 3, text:"cafétaria"}));
    
    result = coll.aggregate([{$match:{_id:3}}, 
                             {$project:{
                                 matches:{
                                    $regexFind:["$text", /fé/]
                             }}}]).toArray();
    assert.eq(result, 
             [{"_id" : 3, "matches" : [{"match" : "fé",
                                        "idx" : 2,
                                        "captures" : ["fé"]
    }]}]); 

    assert.writeOK(coll.insert({_id: 4, text:"text with 02 digits"}));
    
    result = coll.aggregate([{$match:{_id:4}}, 
                             {$project:{
                                 matches:{
                                    $regexFind:["$text", /[0-9]+/]
                             }}}]).toArray();
    assert.eq(result, 
        [{"_id" : 4, "matches" : [{"match" : "02",
                                  "idx" : 10,
                                  "captures" : ["02"]
    }]}]);

    //{$regexFind: ["$text", {$regex: /pattern/opts}}]}
    //{$regexFind: ["$text", {$regex: "pattern", $options: "opts"}}]}
    //{$regexFind: ["$text", "$pathToRegexField"]}
    

}());