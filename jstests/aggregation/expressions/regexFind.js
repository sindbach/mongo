// Tests for $regexFind aggregation expression.
(function() {
    "use strict";

    // For assertErrorCode().
    load("jstests/aggregation/extras/utils.js");

    let coll = db.regex_find_expr;
    coll.drop();

    function testRegexFindAgg(key, regexFind, expected){
        let result = coll.aggregate([{"$match":{"_id": key}}, 
                                     {"$project":{
                                       "matches":{"$regexFind": regexFind}}}])
                         .toArray();
        assert.eq(result, expected);
    }

    function testRegexFindAggException(key, regexFind, exceptionCode){
        assertErrorCode(coll, [{"$match":{"_id":key}}, 
                               {"$project":{
                                 "matches":{"$regexFind": regexFind}}}], 
                        exceptionCode); 
    }

    assert.writeOK(coll.insert({_id: 0, text:"Simple Example"}));

    // Basic test : regex string
    testRegexFindAgg(0, {input:"$text", regex:"(m(p))"}, 
                    [{"_id" : 0, 

                      "matches" : [{"match" : "mp", "idx" : 2,
                                    "captures" : ["mp", "p"]}] }]);

    // Basic test: regex object
    testRegexFindAgg(0, {input:"$text", regex:/(m(p))/}, 
                    [{"_id" : 0, 
                      "matches" : [{"match" : "mp", "idx" : 2,
                                    "captures" : ["mp", "p"]}] }]);

    // Test: regex string with global option 
    testRegexFindAgg(0, {input:"$text", regex:"(m(p))", options: "g"}, 
                    [{"_id" : 0, 
                      "matches" : [{"match" : "mp", "idx" : 2,
                                    "captures" : ["mp", "p"]}, 
                                {"match" : "mp", "idx" : 10,
                                    "captures" : ["mp", "p"]}] }]);

    // Test: regex object with global option 
    testRegexFindAgg(0, {input:"$text", regex:/(m(p))/g}, 
                    [{"_id" : 0, 
                      "matches" : [{"match" : "mp", "idx" : 2,
                                    "captures" : ["mp", "p"]}, 
                                {"match" : "mp", "idx" : 10,
                                    "captures" : ["mp", "p"]}] }]);

    // Test: regex object 10 captures
    testRegexFindAgg(0, {input:"$text", 
                         regex:/(S)(i)(m)(p)(l)(e) (Ex)(am)(p)(le)/}, 
                    [{"_id" : 0, 
                      "matches" : [{"match" : "Simple Example", "idx" : 0,
                                  "captures" : ["S", "i", "m", "p", "l", "e", 
                                                "Ex", "am", "p", "le"]}] }]);

    // Test: regex string groups within group 
    testRegexFindAgg(0, {input:"$text", regex:"((S)(i)(m)(p)(l)(e))"}, 
                    [{"_id" : 0, 
                      "matches" : [{"match" : "Simple", "idx" : 0,
                                    "captures" : ["Simple", "S", "i", "m", "p", 
                                                  "l", "e"]}] }]);

    testRegexFindAgg(0, {input:"$text", regex:"(S)(i)(m)((p)(l)(e))"}, 
                    [{"_id" : 0, 
                    "matches" : [{"match" : "Simple", "idx" : 0,
                                    "captures" : ["S", "i", "m", "ple", "p", 
                                                  "l", "e"]}] }]);                   

    assert.writeOK(coll.insert({_id: 1,                                         
                        text:"Some field text with email wan@mongodb.com"}));
    
    // Test: regex email pattern
    testRegexFindAgg(1, {input:"$text", 
                 regex:"([a-zA-Z0-9._-]+)@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+"}, 
                 [{"_id" : 1, "matches" : 
                                 [{"match" : "wan@mongodb.com",
                                   "idx" : 27, "captures" : ["wan"]}] }]); 

    assert.writeOK(coll.insert({_id: 2, text:"cafétéria"}));

    // Test: unicode index counting
    testRegexFindAgg(2, {input:"$text", regex:"té"}, 
                     [{"_id" : 2, "matches" : 
                     [{"match" : "té", "idx" : 4, "captures" : []}]}]); 
    
    assert.writeOK(coll.insert({_id: 3, text:"öi"}));

    testRegexFindAgg(3, {input:"$text", regex:/ö/}, 
                     [{"_id" : 3, "matches" : 
                     [{"match" : "ö", "idx" : 0, "captures" : []}]}]); 

    // Test: regex digits
    assert.writeOK(coll.insert({_id: 4, text:"Text with 02 digits"}));

    testRegexFindAgg(4, {input:"$text", regex: /[0-9]+/ }, 
                    [{"_id" : 4, "matches" : 
                    [{"match" : "02", "idx" : 10, "captures" : []}]}]); 

    assert.writeOK(coll.insert({_id: 6, text:"1,2,3,4,5,6,7,8,9,10"}));

    // Test: regex non capture group                             
    testRegexFindAgg(6, {input:"$text", regex: /^(?:1|a)\,([0-9]+)/ }, 
                    [{"_id" : 6, "matches" : 
                    [{"match" : "1,2", "idx" : 0, "captures" : ["2"]}]}]); 

    assert.writeOK(coll.insert({_id: 7, text:"abc12defgh345jklm"}));
                        
    // Test: regex quantifier                             
    testRegexFindAgg(7, {input:"$text", regex: /[0-9]{3}/ }, 
                    [{"_id" : 7, "matches" : 
                    [{"match" : "345", "idx" : 10, "captures" : []}]}]);                         
                        
    assert.writeOK(coll.insert({_id: 8, text:"This Is Camel Case"}));

    // Test: regex case insensitive option
    testRegexFindAgg(8, {input:"$text", regex: /camel/ }, 
                    [{"_id" : 8, "matches" : []}]);     

    testRegexFindAgg(8, {input:"$text", regex: /camel/i }, 
                    [{"_id" : 8, "matches" : [{"match" : "Camel", "idx" : 8,
                                                   "captures" : [] }]}]);   

    testRegexFindAgg(8, {input:"$text", regex: /camel/, options:"i" }, 
                    [{"_id" : 8, "matches" : [{"match" : "Camel", "idx" : 8,
                                               "captures" : [] }]}]);   

    testRegexFindAgg(8, {input:"$text", regex: "camel", options:"i" }, 
                    [{"_id" : 8, "matches" : [{"match" : "Camel", "idx" : 8,
                                           "captures" : [] }]}]);   

    assert.writeOK(coll.insert({_id: 9, text:"Foo line1\nFoo line2\nFoo line3"}));

    // Test: regex case multi line option
    testRegexFindAgg(9, {input:"$text", regex: /^Foo line\d$/ }, 
                    [{"_id" : 9, "matches" : []}]);   

    testRegexFindAgg(9, {input:"$text", regex: /^Foo line\d$/m }, 
                    [{"_id" : 9, "matches" : 
                                 [{"match" : "Foo line1", "idx" : 0, 
                                   "captures" : [] }]}]);   

    testRegexFindAgg(9, {input:"$text", regex: /^Foo line\d$/mg}, 
                    [{"_id" : 9, "matches" : 
                             [{"match": "Foo line1", "idx": 0, "captures": []},
                              {"match" : "Foo line2", "idx" : 10, "captures": []},
                              {"match" : "Foo line3", "idx" : 20, "captures": []}
                    ]}]);   

    // Test: regex case single line option
    testRegexFindAgg(9, {input:"$text", regex: "Foo.*line" }, 
                    [{"_id" : 9, "matches" : 
                             [{"match" : "Foo line", "idx" : 0, "captures": []}]
                    }]);   

    testRegexFindAgg(9, {input:"$text", regex: "Foo.*line", options:"s" }, 
                    [{"_id" : 9, "matches" : 
                             [{"match" : "Foo line1\nFoo line2\nFoo line", 
                               "idx" : 0, "captures": []}] }]);   

    // Test: regex case extended option
    testRegexFindAgg(9, {input:"$text", regex: "F o o # a comment" }, 
                    [{"_id" : 9, "matches" : [] }]);   

    testRegexFindAgg(9, {input:"$text", regex: "F o o # a comment", options:"x"}, 
                    [{"_id" : 9, "matches" : 
                                [{"match" : "Foo", 
                                "idx" : 0, "captures": []}]
                    }]);   

    testRegexFindAgg(9, {input:"$text", regex: "F o o # a comment \n\n# ignored", options:"x"}, 
                    [{"_id" : 9, "matches" : 
                                [{"match" : "Foo", 
                                "idx" : 0, "captures": []}]
                    }]);

    testRegexFindAgg(9, {input:"$text", regex: "(F o o) # a comment", options:"x"}, 
                    [{"_id" : 9, "matches" : 
                                [{"match" : "Foo", "idx" : 0, 
                                  "captures": ["Foo"]}]
                    }]);   

    testRegexFindAgg(9, {input:"$text", regex: "F o o", options:"mxg" }, 
                    [{"_id" : 9, "matches" : 
                             [{"match" : "Foo", "idx" : 0, "captures": []},
                              {"match" : "Foo", "idx" : 10, "captures": []},
                              {"match" : "Foo", "idx" : 20, "captures": []}]
                    }]);   

    assert.writeOK(coll.insert({_id: 5, text:"Simple Value", pattern:"(m(p))"}));

    // Test: regex pattern from a document field value
    testRegexFindAgg(5, {input:"$text", regex: "$pattern"}, 
                    [{"_id" : 5, "matches" : 
                     [{"match" : "mp", "idx" : 2, "captures" : ["mp", "p"]}]}]); 
                             
    // Test: no matches
    testRegexFindAgg(0, {input:"$text", regex: /foo/}, 
                     [{"_id": 0, "matches":[]}]);

    // Test: regex null
    testRegexFindAgg(0, {input:"$text", regex: null}, 
                     [{"_id": 0, "matches":[]}]); 

    // Test: incorrect object parameter
    testRegexFindAggException(0, "incorrect type", 50770);

    testRegexFindAggException(0, {input:"$text", regex:"(foo)", misc: 1}, 
                              50771);

    // Test: regex object more than 10 captures
    testRegexFindAggException(0, {input:"$text", 
                                  regex: /(S)(i)(m)(p)(l)(e) ((Ex)(am)(p)(le))/}, 
                                50776);
 
    // Test: no input key
    testRegexFindAggException(0, {regex:/(mp)/}, 50772);

    // Test: no regex key
    testRegexFindAggException(0, {input:"$text"}, 50773);

    // Test: no regex key
    testRegexFindAggException(0, {input:"$text", regex:["incorrect"]}, 50774);

    // Test: options specified in 'regex' and in 'options'
    testRegexFindAggException(0, {input:"$text", regex: /(m(p))/i, options: "i"}, 
                              50775);

    // Test: options specified in 'regex' and in 'options'
    testRegexFindAggException(0, {input:"$text", regex: /(m(p))/i, options: "x"}, 
                              50775);

    coll.drop();

    // Test: redaction 
    assert.writeOK(coll.insert({_id: 0, level:"Public Knowledge", 
                                        info:"Company Name"}));
    assert.writeOK(coll.insert({_id: 1, level:"Private Information", 
                                        info:"Company Secret"}));

    let result = coll.aggregate([ {"$project":{ "information": {
                                   "$cond": [ {"$eq": [ 
                                                {"$regexFind": 
                                                    {input:"$level", 
                                                     regex:/public/i}
                                                },
                                                []]},
                                    "REDACTED",
                                    "$info"
                ]}}}]).toArray();
    assert.eq(result, [ 
          { "_id" : 0, "information" : "Company Name" }, 
          { "_id" : 1, "information" : "REDACTED" },  
    ]);

}());
