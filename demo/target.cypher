// Create constraints
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.title IS UNIQUE;

// Create people (Keanu's birth year changed, Nora Ephron removed, Chad Stahelski added)
CREATE (keanu:Person {name: 'Keanu Reeves', born: 1965});
CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967});
CREATE (laurence:Person {name: 'Laurence Fishburne', born: 1961});
CREATE (hugo:Person {name: 'Hugo Weaving', born: 1960});
CREATE (lilly:Person {name: 'Lilly Wachowski', born: 1967});
CREATE (lana:Person {name: 'Lana Wachowski', born: 1965});
CREATE (tom:Person {name: 'Tom Hanks', born: 1956});
CREATE (meg:Person {name: 'Meg Ryan', born: 1961});
CREATE (chad:Person {name: 'Chad Stahelski', born: 1968});
CREATE (joel:Person {name: 'Joel Silver', born: 1952});
CREATE (rob:Person {name: 'Rob Reiner', born: 1947});

// Create movies (The Matrix tagline changed, Sleepless removed, John Wick added)
CREATE (matrix:Movie {title: 'The Matrix', released: 1999, tagline: 'Reality is a thing of the past'});
CREATE (reloaded:Movie {title: 'The Matrix Reloaded', released: 2003, tagline: 'Free your mind'});
CREATE (mail:Movie {title: 'You\'ve Got Mail', released: 1998, tagline: 'At last... a love story you can\'t click off'});
CREATE (wick:Movie {title: 'John Wick', released: 2014, tagline: 'Don\'t set him off'});
CREATE (fewgood:Movie {title: 'A Few Good Men', released: 1992, tagline: 'You can\'t handle the truth!'});

// Create reviewers (no unique constraint - demonstrates similarity matching)
// Jessica Thompson: rating_style changed (detailed -> thorough), 3/4 props match = 75% similarity
//   -> coerced into ModifiedNode via similarity matching (above default 50% threshold)
// James Mitchell: removed entirely -> SourceNode (no similar target)
// Sarah Chen: identical -> no diff
// Alex Rodriguez: new -> TargetNode (no similar source)
CREATE (:Reviewer {name: 'Jessica Thompson', url: '/jt', rating_style: 'thorough', joined: 2015});
CREATE (:Reviewer {name: 'Sarah Chen', url: '/sc', rating_style: 'moderate', joined: 2020});
CREATE (:Reviewer {name: 'Alex Rodriguez', url: '/ar', rating_style: 'analytical', joined: 2023});

// Create awards (target only - demonstrates TargetNodeLabel schema diff)
CREATE (:Award {name: 'Academy Award', category: 'Best Visual Effects', year: 1999});

// ACTED_IN relationships
MATCH (keanu:Person {name: 'Keanu Reeves'}), (matrix:Movie {title: 'The Matrix'})
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(matrix);

MATCH (carrie:Person {name: 'Carrie-Anne Moss'}), (matrix:Movie {title: 'The Matrix'})
CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(matrix);

MATCH (laurence:Person {name: 'Laurence Fishburne'}), (matrix:Movie {title: 'The Matrix'})
CREATE (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(matrix);

MATCH (hugo:Person {name: 'Hugo Weaving'}), (matrix:Movie {title: 'The Matrix'})
CREATE (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(matrix);

MATCH (keanu:Person {name: 'Keanu Reeves'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(reloaded);

MATCH (carrie:Person {name: 'Carrie-Anne Moss'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(reloaded);

MATCH (laurence:Person {name: 'Laurence Fishburne'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(reloaded);

MATCH (tom:Person {name: 'Tom Hanks'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (tom)-[:ACTED_IN {roles: ['Joe Fox']}]->(mail);

MATCH (meg:Person {name: 'Meg Ryan'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (meg)-[:ACTED_IN {roles: ['Kathleen Kelly']}]->(mail);

MATCH (keanu:Person {name: 'Keanu Reeves'}), (wick:Movie {title: 'John Wick'})
CREATE (keanu)-[:ACTED_IN {roles: ['John Wick']}]->(wick);

// DIRECTED relationships
MATCH (lilly:Person {name: 'Lilly Wachowski'}), (matrix:Movie {title: 'The Matrix'})
CREATE (lilly)-[:DIRECTED]->(matrix);

MATCH (lana:Person {name: 'Lana Wachowski'}), (matrix:Movie {title: 'The Matrix'})
CREATE (lana)-[:DIRECTED]->(matrix);

MATCH (lilly:Person {name: 'Lilly Wachowski'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (lilly)-[:DIRECTED]->(reloaded);

MATCH (lana:Person {name: 'Lana Wachowski'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (lana)-[:DIRECTED]->(reloaded);

MATCH (chad:Person {name: 'Chad Stahelski'}), (wick:Movie {title: 'John Wick'})
CREATE (chad)-[:DIRECTED]->(wick);

MATCH (rob:Person {name: 'Rob Reiner'}), (fewgood:Movie {title: 'A Few Good Men'})
CREATE (rob)-[:DIRECTED]->(fewgood);

// PRODUCED relationships (identical to source)
MATCH (joel:Person {name: 'Joel Silver'}), (matrix:Movie {title: 'The Matrix'})
CREATE (joel)-[:PRODUCED]->(matrix);

MATCH (joel:Person {name: 'Joel Silver'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (joel)-[:PRODUCED]->(reloaded);

// REVIEWED relationships (reviewer -> movie)
// Jessica's review: different start node hash (her properties changed) + summary changed
//   -> similarity matching on rel props: rating matches (1/2 = 50%) -> ModifiedRelationship
// James's review: removed (James doesn't exist in target) -> SourceRelationship
// Sarah's review: identical reviewer + identical props -> no diff
// Alex's review: new -> TargetRelationship
MATCH (j:Reviewer {name: 'Jessica Thompson'}), (m:Movie {title: 'The Matrix'})
CREATE (j)-[:REVIEWED {summary: 'A groundbreaking film', rating: 95}]->(m);

MATCH (s:Reviewer {name: 'Sarah Chen'}), (m:Movie {title: 'You\'ve Got Mail'})
CREATE (s)-[:REVIEWED {summary: 'A charming rom-com', rating: 90}]->(m);

MATCH (a:Reviewer {name: 'Alex Rodriguez'}), (m:Movie {title: 'The Matrix'})
CREATE (a)-[:REVIEWED {summary: 'A must-watch classic', rating: 92}]->(m);

// FOLLOWS relationships (target only - demonstrates TargetRelationshipType schema diff)
MATCH (s:Reviewer {name: 'Sarah Chen'}), (j:Reviewer {name: 'Jessica Thompson'})
CREATE (s)-[:FOLLOWS]->(j);
