// Create constraints
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.title IS UNIQUE;

// Create people (Keanu's birth year is different, Nora Ephron is missing)
CREATE (keanu:Person {name: 'Keanu Reeves', born: 1965});
CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967});
CREATE (laurence:Person {name: 'Laurence Fishburne', born: 1961});
CREATE (hugo:Person {name: 'Hugo Weaving', born: 1960});
CREATE (lilly:Person {name: 'Lilly Wachowski', born: 1967});
CREATE (lana:Person {name: 'Lana Wachowski', born: 1965});
CREATE (tom:Person {name: 'Tom Hanks', born: 1956});
CREATE (meg:Person {name: 'Meg Ryan', born: 1961});

// New person not in source
CREATE (chad:Person {name: 'Chad Stahelski', born: 1968});

// Create movies (tagline changed for Matrix, Sleepless missing, John Wick added)
CREATE (matrix:Movie {title: 'The Matrix', released: 1999, tagline: 'Reality is a thing of the past'});
CREATE (reloaded:Movie {title: 'The Matrix Reloaded', released: 2003, tagline: 'Free your mind'});
CREATE (mail:Movie {title: 'You\'ve Got Mail', released: 1998, tagline: 'At last... a love story you can\'t click off'});

// Sleepless in Seattle is missing
// John Wick is new
CREATE (wick:Movie {title: 'John Wick', released: 2014, tagline: 'Don\'t set him off'});

// Create relationships
MATCH (keanu:Person {name: 'Keanu Reeves'}), (matrix:Movie {title: 'The Matrix'})
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(matrix);

MATCH (carrie:Person {name: 'Carrie-Anne Moss'}), (matrix:Movie {title: 'The Matrix'})
CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(matrix);

MATCH (laurence:Person {name: 'Laurence Fishburne'}), (matrix:Movie {title: 'The Matrix'})
CREATE (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(matrix);

MATCH (hugo:Person {name: 'Hugo Weaving'}), (matrix:Movie {title: 'The Matrix'})
CREATE (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(matrix);

MATCH (lilly:Person {name: 'Lilly Wachowski'}), (matrix:Movie {title: 'The Matrix'})
CREATE (lilly)-[:DIRECTED]->(matrix);

MATCH (lana:Person {name: 'Lana Wachowski'}), (matrix:Movie {title: 'The Matrix'})
CREATE (lana)-[:DIRECTED]->(matrix);

MATCH (keanu:Person {name: 'Keanu Reeves'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(reloaded);

MATCH (carrie:Person {name: 'Carrie-Anne Moss'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(reloaded);

MATCH (laurence:Person {name: 'Laurence Fishburne'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(reloaded);

MATCH (lilly:Person {name: 'Lilly Wachowski'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (lilly)-[:DIRECTED]->(reloaded);

MATCH (lana:Person {name: 'Lana Wachowski'}), (reloaded:Movie {title: 'The Matrix Reloaded'})
CREATE (lana)-[:DIRECTED]->(reloaded);

MATCH (tom:Person {name: 'Tom Hanks'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (tom)-[:ACTED_IN {roles: ['Joe Fox']}]->(mail);

MATCH (meg:Person {name: 'Meg Ryan'}), (mail:Movie {title: 'You\'ve Got Mail'})
CREATE (meg)-[:ACTED_IN {roles: ['Kathleen Kelly']}]->(mail);
// Nora Ephron DIRECTED relationship is missing (she doesn't exist)

// New relationships for John Wick
MATCH (keanu:Person {name: 'Keanu Reeves'}), (wick:Movie {title: 'John Wick'})
CREATE (keanu)-[:ACTED_IN {roles: ['John Wick']}]->(wick);

MATCH (chad:Person {name: 'Chad Stahelski'}), (wick:Movie {title: 'John Wick'})
CREATE (chad)-[:DIRECTED]->(wick);
